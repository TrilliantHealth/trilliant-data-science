import json
from logging import getLogger
from pathlib import Path
from typing import (
    Callable,
    Collection,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import networkx as nx
import pandas as pd
import pkg_resources
import pyarrow
import pyarrow.parquet
import setuptools.command.build_py

from thds.tabularasa.loaders.util import PandasParquetLoader, default_parquet_package_data_path
from thds.tabularasa.schema import load_schema
from thds.tabularasa.schema.compilation import (
    render_attrs_module,
    render_attrs_sqlite_schema,
    render_pandera_module,
    render_pyarrow_schema,
    render_sql_schema,
    write_if_ast_changed,
    write_sql,
)
from thds.tabularasa.schema.files import LocalDataSpec, TabularFileSource
from thds.tabularasa.schema.metaschema import (
    ADLSRef,
    BuildOptions,
    LocalRef,
    RawDataDependencies,
    ReferenceDataRef,
    Schema,
    Table,
    TabularTextFileRef,
    TransientReferenceDataRef,
    is_build_time_package_table,
)
from thds.tabularasa.schema.util import predecessor_graph

from .adls import ADLSDownloadResult, sync_adls_data
from .sqlite import populate_sqlite_db
from .tabular import PandasCSVLoader
from .util import (
    PARQUET_FORMAT_VERSION,
    arrow_table_for_parquet_write,
    hash_file,
    import_data_preprocessor,
    package_data_file_size,
)

ResourceRef = Union[ADLSRef, ReferenceDataRef, LocalRef]
# do-nothing computational DAG nodes - we exclude these from the compute order for better visibility on
# the number of meaningful build steps
NoOpRefTypes = (TabularTextFileRef, LocalRef)

METADATA_FILE = "meta.json"


class ReferenceDataBuildCommand(setuptools.command.build_py.build_py):
    """Use in your setup.py as follows:

    .. code-block:: python

        from setuptools import setup

        my_build_cmd = ReferenceDataBuildCommand.with_options(
            package_name="my_package",
            schema_path="path/inside/my_package/to/my/schema.yaml",
        )

        setup(
            ...
            cmdclass={"build_py": my_build_cmd}
            ...
        )
    """

    package_name: str
    schema_path: str
    for_setup_py_build: bool
    schema: Schema

    @classmethod
    def with_options(
        cls,
        *,
        package_name: str,
        schema_path: str,
        for_setup_py_build: bool = True,
    ) -> Type["ReferenceDataBuildCommand"]:
        """Supply parameters specifying a reference data build for a specific package

        :param package_name: name of the package where the data is to be defined and stored
        :param schema_path: path to the schema relative to the package root; should be a YAML file
          compatible with the structure of `thds.tabularasa.schema.metaschema.Schema`
        :param for_setup_py_build: if `True` (the default), this indicates that this class is being used
          for packaging/building in the context of a setuptools build. This will cause some steps to
          execute that may not be wanted in other workflows, in which case it should be set to `False`
        :return: a `ReferenceDataBuildCommand` subclass with the provided fields populated
        """
        namespace = locals()
        namespace.pop("cls")
        return type(
            cls.__name__,
            (cls,),
            namespace,
        )  # type: ignore

    @property
    def options(self) -> BuildOptions:
        assert self.schema.build_options is not None
        return self.schema.build_options

    def __init__(self, *args, **kwargs):
        self.schema = load_schema(
            package=self.package_name,
            schema_path=self.schema_path,
            require_preprocessors=self.for_setup_py_build,
            require_data_resources=self.for_setup_py_build,
        )
        self.derived_code_submodule_dir: Path = Path(
            pkg_resources.resource_filename(
                self.package_name,
                self.options.derived_code_submodule.replace(".", "/"),
            )
        )
        assert self.schema.build_options is not None, "Can't build without build_options being specified"

        if self.for_setup_py_build:
            super().__init__(*args, **kwargs)

    def run(self):
        self.write_derived_source_code()
        super().run()

    def build_package_data(self, check_hash: bool = True, tables: Optional[Collection[str]] = None):
        # derive and write package data
        package_data_paths: List[str] = [self.schema_path]

        if tables is None:
            build_kw = {}
        else:
            unknown_tables = {t for t in tables if t not in self.schema.tables}
            if unknown_tables:
                raise KeyError(f"Unknown tables: {','.join(unknown_tables)}")
            build_kw = dict(table_predicate=lambda table: table.name in tables)  # type: ignore

        if any(
            table.build_time_installed and not table.transient for table in self.schema.tables.values()
        ):
            assert (
                self.options.package_data_dir is not None and self.options.transient_data_dir is not None
            ), "package_data_dir or transient_data_dir specified; can't write tables"
            package_data_table_paths, _ = write_package_data_tables(
                self.schema,
                package=self.package_name,
                output_data_dir=self.options.package_data_dir,
                transient_data_dir=self.options.transient_data_dir,
                check_hash=check_hash,
                validate_transient_tables=self.options.validate_transient_tables,
                **build_kw,
            )
            package_data_paths.extend(
                get_data_files_to_package(
                    self.package_name,
                    package_data_table_paths,
                    size_limit=self.options.package_data_file_size_limit,
                )
            )

        if self.options.sqlite_data:
            # now initialize database and load reference data into database
            assert (
                self.options.sqlite_db_path is not None
            ), "No sqlite_db_path specified; can't populate db"
            assert (
                self.options.package_data_dir is not None
            ), "No package_data_dir specified; can't populate db"
            assert (
                self.options.transient_data_dir is not None
            ), "No transient_data_dir specified; can't populate db"
            populate_sqlite_db(
                self.schema,
                db_package=self.package_name,
                db_path=self.options.sqlite_db_path,
                data_package=self.package_name,
                data_dir=self.options.package_data_dir,
                transient_data_dir=self.options.transient_data_dir,
                check_hash=check_hash,
                **build_kw,  # type: ignore
            )
            package_data_paths.append(self.options.sqlite_db_path)

        package_data = {
            "": [METADATA_FILE, "py.typed"],
            self.package_name: package_data_paths,
        }
        if hasattr(self, "package_data") and self.package_data is not None:
            self.package_data = {
                key: self.package_data.get(key, []) + package_data.get(key, [])
                for key in set(self.package_data).union(package_data)
            }
        else:
            self.package_data = package_data

        getLogger(__name__).info(f"package_data set to:\n{json.dumps(self.package_data, indent=4)}")

    def write_derived_source_code(self):
        # attrs classes needed for sqlite interface
        if self.options.attrs or self.options.sqlite_interface:
            attrs_source = render_attrs_module(
                self.schema,
                package=self.package_name,
            )
            write_if_ast_changed(attrs_source, self.derived_code_submodule_dir / "attrs.py")
        if self.options.pandas:
            pandas_source = render_pandera_module(
                self.schema,
                package=self.package_name,
            )
            write_if_ast_changed(pandas_source, self.derived_code_submodule_dir / "pandas.py")
        if self.options.pyarrow:
            pyarrow_source = render_pyarrow_schema(self.schema)
            write_if_ast_changed(pyarrow_source, self.derived_code_submodule_dir / "pyarrow.py")
        if self.options.sqlite_interface:
            attrs_sqlite_source = render_attrs_sqlite_schema(
                self.schema,
                package=self.package_name,
                db_path=self.options.sqlite_db_path or "",
            )
            write_if_ast_changed(
                attrs_sqlite_source, self.derived_code_submodule_dir / "attrs_sqlite.py"
            )
            sql_table_source, sql_index_source = render_sql_schema(self.schema)
            write_sql(sql_table_source, self.derived_code_submodule_dir / "table.sql")
            write_sql(sql_index_source, self.derived_code_submodule_dir / "index.sql")


def write_package_data_tables(
    schema: Schema,
    output_data_dir: str,
    transient_data_dir: str,
    package: str,
    check_hash: bool = True,
    table_predicate: Callable[[Table], bool] = is_build_time_package_table,
    validate_transient_tables: bool = False,
) -> Tuple[List[str], List[str]]:
    """This is the main routine for building all derived package data.

    The main steps in this process are:
    1) compute the computational DAG represented by the dependencies in the schema
    2) determine which of the reference table nodes in the DAG have already been computed and can be
       loaded from disk, by using a file existence check and optionally a hash check (this saves time
       downloading ADLS resources and computing derived data in local builds). Dependency links are
       removed for local reference table nodes which have been precomputed, since these can be simply
       loaded from their local package data files. Any nodes remaining in the DAG with no upstream
       dependencies and no downstream dependents remaining to be computed are finally removed
       from the DAG.
    3) traverse the DAG in topological order and compute the resources associated with the nodes. For
       ADLS resources, this means fetching the remote files from ADLS to a local build cache. For derived
       package data tables, it means
         a) importing and calling the associated preprocessor function on the upstream ADLS, local file,
            and reference table dependencies, when the dependencies are a `RawDataDependencies` instance
         b) loading the table from a tabular text file, when the dependencies are a `TabularFileSource`
            instance
       In that case, the resulting derived table is saved as a strictly-typed parquet file in
       `output_data_dir` as package data for `package`.

    For remote builds, there will be no cache populated and so the pruning in step 2 has no effect. For
    local builds, the speed of development will benefit from the local cache being populated on the first
    build.

    The table_predicate argument can be used to filter only a subset of tables for computation and
    packaging. By default, all tables marked as build-time-installed and not transient (and all of their
    recursive dependencies) are computed.

    :return: 2 lists of *package data* paths to the derived table parquet files, one for tables packaged
      with interfaces and another for transient tables. These can be used e.g. to specify package data
      paths in a build.
    """
    _LOGGER = getLogger(__name__)
    _LOGGER.info("Computing derived reference data tables")
    compute_order, precomputed_tables = _computation_order_and_dependencies(
        schema,
        package=package,
        output_data_dir=output_data_dir,
        transient_data_dir=transient_data_dir,
        check_hash=check_hash,
        table_predicate=table_predicate,
    )

    # optimization for DIY garbage collection as prior nodes are no longer needed for future nodes;
    # we can remove a computed table from the cache as soon as we pass its last required index
    last_indexes = {}
    for ix, (ref, deps) in enumerate(compute_order):
        last_indexes[ref] = ix
        for dep in deps:
            last_indexes[dep] = ix

    # store intermediate results here
    adls_cache: Dict[str, List[ADLSDownloadResult]] = {}
    ref_cache: Dict[str, pd.DataFrame] = {}

    if len(compute_order):
        _LOGGER.info("Traversing data dependency DAG and computing tables")

    # keep track of paths to tables that have already been computed
    package_data_paths: List[str] = []
    transient_data_paths: List[str] = []
    for table_ref, path in precomputed_tables.items():
        if isinstance(table_ref, TransientReferenceDataRef):
            transient_data_paths.append(path)
        else:
            package_data_paths.append(path)

    # finally loop over package data resources that need computing and fetch/compute them
    for ix, (ref, deps) in enumerate(compute_order):
        if isinstance(ref, NoOpRefTypes):
            # shouldn't happen because we filter them in determining the compute order -
            # just for completeness
            continue
        elif isinstance(ref, ADLSRef):
            # download ADLS files
            _LOGGER.info(f"Syncing ADLS resource {ref}")
            adls_cache[str(ref)] = sync_adls_data(schema.remote_data[str(ref)])
        elif isinstance(ref, ReferenceDataRef):
            table = schema.tables[str(ref)]
            if table.transient:
                data_dir = transient_data_dir
                paths = transient_data_paths
            else:
                data_dir = output_data_dir
                paths = transient_data_paths

            if ref not in precomputed_tables:
                # compute package data table from dependencies
                df = _compute_dependent_table(table, ref_cache, adls_cache, schema.local_data)
                _LOGGER.info("Saving newly computed table %s", ref)
                _save_as_package_data(df, table, package, data_dir)
            else:
                df = None

            package_data_path = default_parquet_package_data_path(table.name, data_dir)
            paths.append(package_data_path)

            # garbage collection
            for dep in deps:
                if isinstance(dep, ReferenceDataRef) and last_indexes[dep] <= ix:
                    _LOGGER.info(
                        "Collecting table %s which is not needed in any downstream build step",
                        dep,
                    )
                    del ref_cache[str(dep)]

            if last_indexes[ref] <= ix:
                if df is not None:
                    _LOGGER.info(
                        "Collecting table %s which is not needed in any downstream build step",
                        ref,
                    )
                    del df
            else:
                # load in from disk for downstream computations - loading from disk ensures exactly the
                # same dataframe whether the above block was run or not
                _LOGGER.info(
                    "Loading table %s from disk for use in next %d build steps",
                    table.name,
                    last_indexes[ref] - ix,
                )
                validate = validate_transient_tables and table.transient
                pandas_loader = PandasParquetLoader.from_schema_table(
                    table, package=package, data_dir=data_dir, derive_schema=validate
                )
                df = pandas_loader(validate=validate)
                ref_cache[str(ref)] = df

    return package_data_paths, transient_data_paths


def _compute_dependent_table(
    table: Table,
    ref_cache: Mapping[str, pd.DataFrame],
    adls_cache: Mapping[str, List[ADLSDownloadResult]],
    local_cache: Mapping[str, LocalDataSpec],
) -> pd.DataFrame:
    _LOGGER = getLogger(__name__)
    if isinstance(table.dependencies, TabularFileSource):
        pandas_loader = PandasCSVLoader(table)
        _LOGGER.info(
            "Translating tabular text file at %s to parquet for table %s",
            table.dependencies.filename,
            table.name,
        )
        df = pandas_loader(validate=False)
    elif isinstance(table.dependencies, RawDataDependencies):
        ref_deps = table.dependencies.reference
        adls_deps = table.dependencies.adls
        local_deps = table.dependencies.local
        _LOGGER.info(
            "Computing table %s from reference dependencies [%s] local dependencies [%s] and ADLS "
            "dependencies [%s]",
            table.name,
            ", ".join(ref_deps),
            ", ".join(local_deps),
            ", ".join(adls_deps),
        )
        preprocessor = import_data_preprocessor(table.dependencies.preprocessor)
        df = preprocessor(
            {dep: ref_cache[dep] for dep in ref_deps},
            {dep: adls_cache[dep] for dep in adls_deps},
            {dep: local_cache[dep] for dep in local_deps},
        )
    else:
        raise ValueError(f"Can't compute table {table.name}: no dependencies defined")

    return df


def _save_as_package_data(
    df: pd.DataFrame,
    table: Table,
    package_name: str,
    data_dir: str,
) -> Path:
    """NOTE: This function mutates `df` but is only ever called in one place in
    `write_package_data_tables`, just before the reference to `df` is collected."""
    file_path = Path(
        pkg_resources.resource_filename(
            package_name, default_parquet_package_data_path(table.name, data_dir)
        )
    )
    getLogger(__name__).info("Writing table %s to %s", table.name, file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # cast complex types (e.g. dicts) to types that pyarrow can interpret for writing to parquet
    # cast some other compatible dtypes or warn if it can't be done safely
    # reset index and sort by index columns
    # ensure exact parquet schema by using pyarrow
    arrow = arrow_table_for_parquet_write(df, table)
    pyarrow.parquet.write_table(arrow, file_path, compression="snappy", version=PARQUET_FORMAT_VERSION)
    return file_path


def _computation_order_and_dependencies(
    schema: Schema,
    package: str,
    output_data_dir: str,
    transient_data_dir: str,
    table_predicate: Callable[[Table], bool],
    check_hash: bool = True,
) -> Tuple[List[Tuple[ResourceRef, List[ResourceRef]]], Dict[ReferenceDataRef, str]]:
    _LOGGER = getLogger(__name__)
    # guaranteed to be a DAG by load-time validation
    dag = schema.dependency_dag(table_predicate)

    precomputed_tables: Dict[ReferenceDataRef, str] = dict()

    if check_hash:
        _LOGGER.info("Checking hashes of existing derived tables")

    # determine dependent tables that have already been computed by hash
    for table in schema.filter_tables(lambda t: t.graph_ref in dag):
        derived_pqt_md5 = table.md5
        pqt_package_data_path = default_parquet_package_data_path(
            table.name,
            data_dir=transient_data_dir if table.transient else output_data_dir,
        )

        if check_hash:
            if derived_pqt_md5 is not None and pkg_resources.resource_exists(
                package,
                pqt_package_data_path,
            ):
                with pkg_resources.resource_stream(package, pqt_package_data_path) as f:
                    if hash_file(f) == derived_pqt_md5:
                        precomputed_tables[table.graph_ref] = pqt_package_data_path
                    else:
                        _LOGGER.warning(
                            "MD5 of file %s in package %s for table %s doesn't match expected value; "
                            "cannot safely skip computation",
                            pqt_package_data_path,
                            package,
                            table.name,
                        )
            elif derived_pqt_md5 is None:
                _LOGGER.warning(
                    "No MD5 hash defined for table %s; it will be re-computed on every build; add a hash"
                    " of the generated file %s in the %s package to the dependencies block of the "
                    "schema to prevent this",
                    table.name,
                    pqt_package_data_path,
                    package,
                )
        elif pkg_resources.resource_exists(package, pqt_package_data_path):
            if derived_pqt_md5 is not None:
                _LOGGER.warning(
                    "Ignoring MD5 hash for table %s since check_hash=False was passed; its associated "
                    "package data exists at %s and will not be regenerated regardless of its hash",
                    table.name,
                    pqt_package_data_path,
                )
            precomputed_tables[table.graph_ref] = pqt_package_data_path

    # we don't need to compute dependencies for tables that have been computed - they can be loaded
    # from disk
    for table_ref in precomputed_tables:
        _LOGGER.info(
            f"{table_ref!r} is pre-computed and can be loaded from package data; removing dependency "
            f"links"
        )
        # don't need to compute dependencies for this table; can load from disk
        for upstream in list(dag.predecessors(table_ref)):
            dag.remove_edge(upstream, table_ref)

    # anything not required by our intended tables can be removed
    requested_tables = set(
        table.graph_ref for table in schema.filter_tables(table_predicate)
    ).difference(precomputed_tables)
    filtered_dag = predecessor_graph(dag, requested_tables)
    for ref in set(dag).difference(filtered_dag):
        _LOGGER.info(
            f"Safely skipping computation of {ref!r}; no downstream dependencies remaining to compute"
        )
        dag.remove_node(ref)

    def is_build_step(ref):
        if isinstance(ref, NoOpRefTypes):
            return False
        if isinstance(ref, ReferenceDataRef):
            return ref not in precomputed_tables or any(filtered_dag.successors(ref))
        return True

    load_order = [
        (ref, list(filtered_dag.predecessors(ref)))
        for ref in filter(is_build_step, nx.topological_sort(filtered_dag))
    ]
    _LOGGER.info(f"Final build stage order: {[ref for ref, deps in load_order]}")
    return load_order, precomputed_tables


def get_data_files_to_package(
    package: str,
    package_data_paths: Iterable[str],
    size_limit: Optional[int],
) -> Iterable[str]:
    _LOGGER = getLogger(__name__)
    if size_limit is None:
        _LOGGER.info("Packaging all data files since no size limit is specified")
        yield from package_data_paths
    else:
        size_limit_ = cast(int, size_limit)  # mypy needs this for some weird reason

        def size_filter(package_data_path: str, size_limit: int = size_limit_) -> bool:
            if package_data_file_size(package, package_data_path) > size_limit:
                _LOGGER.info(
                    f"Filtering out {package_data_path}. File is too large to package "
                    "but will be stored remote blob store"
                )
                return False
            return True

        yield from filter(size_filter, package_data_paths)
