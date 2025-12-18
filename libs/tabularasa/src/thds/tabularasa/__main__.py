import logging
import shutil
import subprocess
import sys
import tempfile
from copy import copy
from enum import Enum
from functools import partial
from itertools import repeat
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, NamedTuple, Optional, Set, Tuple, Type, Union, cast

import networkx as nx
import pkg_resources

from thds.core import link, parallel
from thds.tabularasa.data_dependencies.adls import (
    ADLSFileIntegrityError,
    ADLSFileSystem,
    adls_filesystem,
    sync_adls_data,
)
from thds.tabularasa.data_dependencies.build import ReferenceDataBuildCommand, populate_sqlite_db
from thds.tabularasa.diff import data as data_diff
from thds.tabularasa.diff import schema as schema_diff
from thds.tabularasa.diff import summary as diff_summary
from thds.tabularasa.loaders import parquet_util
from thds.tabularasa.loaders.util import (
    PandasParquetLoader,
    default_parquet_package_data_path,
    hash_file,
)
from thds.tabularasa.schema import load_schema, metaschema
from thds.tabularasa.schema.compilation import (
    render_attrs_module,
    render_attrs_sqlite_schema,
    render_pandera_module,
    render_pyarrow_schema,
    render_sphinx_docs,
    render_sql_schema,
)
from thds.tabularasa.schema.util import all_predecessors, all_successors

try:
    from bourbaki.application.cli import CommandLineInterface, cli_spec
except ImportError:

    # stand-in decorators
    def noop_decorator(obj):
        return obj

    def noop_decorator_factory(obj):
        return noop_decorator

    config_top_level = define_cli = noop_decorator
    output_handler = noop_decorator_factory
    noncommand = noop_decorator
    cli = None
else:
    # increase default log verbosity
    # this ensures all log messages at INFO level or greater are rendered,
    # and that tracebacks are always shown
    import bourbaki.application.cli.main as _bourbaki

    _bourbaki.MIN_VERBOSITY = _bourbaki.TRACEBACK_VERBOSITY = _bourbaki.LOG_LEVEL_NAMES.index("INFO")

    cli = CommandLineInterface(
        prog="tabularasa",
        require_options=False,
        require_subcommand=True,
        implicit_flags=True,
        use_verbose_flag=True,
        require_config=False,
        add_init_config_command=True,
        use_config_file="tabularasa.yaml",
        package="thds.tabularasa",
    )
    # decorators
    define_cli = cli.definition
    output_handler = cli_spec.output_handler
    config_top_level = cli_spec.config_top_level
    noncommand = cli_spec.noncommand

try:
    from ruamel.yaml import YAML
except ImportError:

    import yaml

    load_yaml = yaml.safe_load
    dump_yaml = yaml.safe_dump
else:

    def _yaml():
        yaml = YAML()
        yaml.preserve_quotes = True  # type: ignore[assignment]
        yaml.width = 100  # type: ignore[assignment]
        return yaml

    def load_yaml(stream):
        return _yaml().load(stream)

    def dump_yaml(data, stream):  # type: ignore
        _yaml().dump(data, stream)


DEFAULT_GRAPHVIZ_FORMAT = "svg"
RED, GREEN, YELLOW, BLUE = "#FFAB99", "#99FFDE", "#EDFF99", "#b3f0ff"
DAG_NODE_COLORS: Dict[Type, str] = {
    metaschema.ADLSRef: RED,
    metaschema.LocalRef: YELLOW,
    metaschema.TabularTextFileRef: YELLOW,
    metaschema.TransientReferenceDataRef: BLUE,
    metaschema.ReferenceDataRef: GREEN,
}


class CompilationTarget(Enum):
    pandas = "pandas"
    sqlite = "sqlite"
    pyarrow = "pyarrow"
    attrs = "attrs"
    attrs_sqlite = "attrs_sqlite"


class DataFileHashes(NamedTuple):
    actual: Optional[str]
    expected: Optional[str]


class TableSyncData(NamedTuple):
    local_path: Path
    blob_store: metaschema.RemoteBlobStoreSpec
    md5: str

    @property
    def remote_path(self) -> str:
        return self.remote_data_spec.paths[0].name

    @property
    def remote_data_spec(self) -> metaschema.ADLSDataSpec:
        data_spec = self.blob_store.data_spec(self.md5)
        return data_spec

    @property
    def local_file_exists(self) -> bool:
        return self.local_path.exists()

    @property
    def remote_file_system(self) -> ADLSFileSystem:
        return adls_filesystem(self.blob_store.adls_account, self.blob_store.adls_filesystem)

    def local_file_md5(self) -> Optional[str]:
        return hash_file(self.local_path) if self.local_file_exists else None

    def remote_file_exists(self) -> bool:
        return self.remote_file_system.file_exists(self.remote_path)


def print_source(source, *, output: Optional[Path] = None):
    if output is None:
        outfile = sys.stdout
    else:
        outfile = open(output, "w")

    print(source, file=outfile)

    if output is not None:
        outfile.close()


def print_file_hashes_status(hashes: Dict[str, DataFileHashes]):
    ready_for_packaging = True
    for name, hs in sorted(hashes.items(), key=lambda kv: kv[0]):
        if hs.actual != hs.expected:
            if hs.actual:
                if hs.expected:
                    print(f"{name}: actual md5 {hs.actual} != expected md5 {hs.expected}")
                    ready_for_packaging = False
                else:
                    print(f"{name}: actual md5 {hs.actual}; NO md5 IN SCHEMA")
            else:
                print(f"{name}: NO FILE")
                ready_for_packaging = False
        else:
            print(f"{name}: ✔")

    if not ready_for_packaging:
        raise Exception("package data files or schema are not ready for packaging")


def print_list(it: Iterable):
    for i in it:
        print(i)


def print_schema_diff_summary(
    diff: schema_diff.SchemaDiff,
    *,
    exit_code: bool = False,
    heading_level: int = 0,
    tablefmt: str = diff_summary.DEFAULT_TABLEFMT,
):
    """Print the schema diff summary to stdout and raise an exception if there are positive diffs

    :param diff: the schema diff to summarize
    :param exit_code: if passed, exit with code 1 if there is a positive diff (similar to `git diff --exit-code`)
    :param tables: if passed, only show diffs for these tables. Note that a table may not be shown if it is
      transient and the `transient` flag is not passed; a warning is raised in this case
    :param transient: if passed, show diffs for transient tables
    :param heading_level: increase this to render smaller headings on the markdown sections
    :param tablefmt: the table format to use for the markdown tables, as understood by `tabulate`
    """
    positive_diff = False
    for section in diff_summary.markdown_schema_diff_summary(
        diff,
        heading_level=heading_level,
        tablefmt=tablefmt,
    ):
        print(section, end="\n\n")
        positive_diff = True
    if positive_diff and exit_code:
        exit(1)


def print_data_diff_summaries(
    data_diffs: Iterator[Tuple[metaschema.Identifier, data_diff.DataFrameDiff]],
    *,
    exit_code: bool = False,
    verbose: bool = False,
    value_detail: bool = False,
    value_detail_min_count: int = 0,
    heading_level: int = 0,
    tablefmt: str = diff_summary.DEFAULT_TABLEFMT,
    floatfmt: str = diff_summary.DEFAULT_FLOATFMT,
):
    """Print summaries of data diffs for a sequence of updated tables

    :param data_diffs: an iterator of tuples of table names and their corresponding data diffs
    :param exit_code: if True, exit with code 1 if there is a positive diff (similar to `git diff --exit-code`)
    :param verbose: if True, show detailed row change status counts; otherwise show only single-column
      change counts
    :param value_detail: if True, show detailed value change counts; otherwise show only statistics of the
      types of changes as determined by the `verbose` flag
    :param value_detail_min_count: minimum number of instances of a specific value update to show value-level
      detail for. No effect when `value_detail` is False
    :param heading_level: increase this to render smaller headings on the markdown sections
    :param tablefmt: the table format to use for the markdown tables, as understood by `tabulate`
    :param floatfmt: the float format to use for the markdown tables, as understood by `tabulate`
    """
    positive_diff = False
    for table_name, d_diff in data_diffs:
        for section in diff_summary.markdown_dataframe_diff_summary(
            d_diff,
            table_name,
            verbose,
            value_detail=value_detail,
            value_detail_min_count=value_detail_min_count,
            heading_level=heading_level,
            tablefmt=tablefmt,
            floatfmt=floatfmt,
        ):
            positive_diff = True
            print(section, end="\n\n")

    if positive_diff and exit_code:
        exit(1)


def to_graphviz(
    dag: nx.DiGraph,
    vertical: bool = False,
    ranksep: float = 1.0,
    nodesep: float = 1.0,
    fontsize: int = 12,
    fontname: str = "Courier",
    compact: bool = False,
):
    try:
        from pygraphviz import AGraph
    except ImportError:
        raise RuntimeError("dag visulization requires `pygraphviz`")

    title = "Reference Data dependency DAG"
    g = AGraph(
        directed=True,
        name=title,
        rankdir="TB" if vertical else "LR",
        fontsize=fontsize * 3,
        fontname=fontname,
        label=title,
        labelloc="t",
        ranksep=str(ranksep) + " equally",
        nodesep=str(nodesep),
    )
    g.node_attr["shape"] = "box"
    g.node_attr["fontname"] = fontname
    g.node_attr["fontsize"] = fontsize

    for node, attrs in dag.nodes(data=True):
        color = DAG_NODE_COLORS.get(type(node))
        name = repr(node)
        g.add_node(
            name,
            label=name,
            fillcolor=color,
            style="bold" if attrs.get("initial") else "filled",
        )

    g.add_edges_from((repr(head), repr(tail)) for head, tail in dag.edges)

    if compact:
        # add invisible edges between components to put them on separate levels
        def terminal_nodes(nodes: Iterable[metaschema.CustomStr], initial: bool) -> Iterable[str]:
            lookup = dag.pred if initial else dag.succ
            return (repr(node) for node in nodes if not len(lookup[node]))

        def balanced_layers(dag: nx.DiGraph) -> List[Set[metaschema.CustomStr]]:
            components = cast(
                List[Set[metaschema.CustomStr]],
                sorted(nx.connected_components(dag.to_undirected()), key=len),
            )
            target_size = len(components[-1])
            layers = [components[0]]
            for nodes in components[1:]:
                component_to_merge = layers[-1]
                if len(component_to_merge) + len(nodes) <= target_size:
                    component_to_merge.update(nodes)
                else:
                    layers.append(nodes)
            return layers

        layers = balanced_layers(dag)
        for i, (layer1, layer2) in enumerate(zip(layers, layers[1:]), 1):
            sep_node = f"Layer({i})"
            g.add_node(sep_node, style="invis")
            g.add_edges_from(zip(terminal_nodes(layer1, False), repeat(sep_node)), style="invis")
            g.add_edges_from(zip(repeat(sep_node), terminal_nodes(layer2, True)), style="invis")

    g.layout(prog="dot")
    return g


def write_dependency_dag(
    dag: nx.DiGraph,
    *,
    output: Optional[Path] = None,
    format: Optional[str] = None,
    vertical: bool = False,
    fontsize: int = 12,
    compact: bool = False,
):
    """Save a visualization of the dependency DAG using pygraphviz

    :param dag: networkx graph representing the DAG
    :param output: the file to write the visualization to. If not passed, a temp file will be created.
    :param format: the format to save the image as (e.g. svg, png); if not passed, it will be inferred
      from the output path name. When that is not passed, svg will be used.
    :param vertical: orient the DAG visualization from top to bottom? (default is left to right)
    :param fontsize: font size of text (e.g. table and resource names) in the visualization
    :param compact: if True, put separate connected components of the DAG on separate levels (vertical
      or horizontal depending on the orientation). For wide DAGs with many components this can result in
      a much more compact representation.
    """
    graphviz = to_graphviz(dag, vertical=vertical, fontsize=fontsize, compact=compact)
    if output is None:
        format = format.lower() if format else DEFAULT_GRAPHVIZ_FORMAT
        output = Path(tempfile.mkstemp(suffix=f".{format}")[1])
        print(f"Created teporary file at {output}; will save in PNG format")

    print(f"Saving DAG visualization to {output}", file=sys.stderr)
    graphviz.draw(str(output), format=format, prog="dot")
    try:
        subprocess.run(["open", str(output)])
    except Exception as e:
        print(
            f"Couldn't run `open {output}` ({e}); open the file manually",
            file=sys.stderr,
        )


@define_cli
@config_top_level
class ReferenceDataManager:
    def __init__(
        self,
        *,
        package: str,
        schema_path: str,
        repo_root: Optional[Path] = None,
        require_editable_install: bool = False,
    ):
        """Utilities for managing, installing, validating, and inspecting reference data

        :param package: name of the package where the data is to be defined and stored and where the
          schema should be read from
        :param schema_path: path to the schema relative to the package root; should be a YAML file
          compatible with `reference_data.schema.metaschema.Schema`
        :param repo_root: path to the root of the local repository. If not supplied, it will be set to
          the current working directory
        :param require_editable_install: Fail if the package is not installed in editable mode? This is
          generally what you want when developing/publishing. However there are use cases where one may
          wish to edit a hard install of the package in place, e.g. when syncing data files to the
          installed package data directory, in which case this may remain as the default `False` value.
        :return: a `ReferenceDataBuildCommand` subclass with the provided fields populated
        """
        self.build_command = ReferenceDataBuildCommand.with_options(
            package_name=package,
            schema_path=schema_path,
            for_setup_py_build=False,
        )()
        self.logger = logging.getLogger(__name__)
        if repo_root is None:
            self.repo_root = Path.cwd().resolve()
        else:
            self.repo_root = repo_root.resolve()

        if require_editable_install:
            self.check_editable_install()

    def check_editable_install(self):
        """Ensure that the package being built is installed in an editable mode; otherwise the operations
        defined in this interface may not have the intended effects."""
        local_data_dir = Path(pkg_resources.resource_filename(self.package, ""))
        if not str(local_data_dir).startswith(str(self.repo_root)):
            msg = (
                f"Package {self.package} appears not to be installed in editable mode; this could result"
                "for example in incorrect file hashes or a corrupted package installation"
            )
            self.logger.exception(msg)
            raise RuntimeError(msg)
        else:
            self.logger.info(f"Check passed - package {self.package} is installed in editable mode")

    def load_raw_schema(self):
        """Round-trippable load of the schema YAML file, for development operations where the file needs
        to be edited while preserving style and comments"""
        self.logger.info("Loading round-trippable raw schema")
        with pkg_resources.resource_stream(self.package, self.schema_path) as f:
            return load_yaml(f)

    @property
    def schema(self) -> metaschema.Schema:
        return self.build_command.schema

    @property
    def build_options(self) -> metaschema.BuildOptions:
        options = self.schema.build_options
        assert options is not None
        return options

    @property
    def package(self) -> str:
        return self.build_command.package_name

    @property
    def schema_path(self) -> str:
        return self.build_command.schema_path

    @property
    def package_data_dir(self) -> Optional[str]:
        return self.build_options.package_data_dir

    @property
    def transient_data_dir(self) -> Optional[str]:
        return self.build_options.transient_data_dir

    @property
    def sqlite_db_path(self) -> Optional[str]:
        return self.build_options.sqlite_db_path

    @property
    def repo_url(self):
        return self.build_options.repo_url

    @property
    def table_docs_dir(self):
        return self.build_options.table_docs_dir

    @property
    def type_docs_path(self):
        return self.build_options.type_docs_path

    @property
    def source_docs_path(self):
        return self.build_options.source_docs_path

    @property
    def curation_badge_path(self):
        return self.build_options.curation_badge_path

    def data_path_for(self, table: Union[str, metaschema.Table]) -> Path:
        table_ = self.schema.tables[table] if isinstance(table, str) else table
        data_dir = self.transient_data_dir if table_.transient else self.package_data_dir
        assert data_dir is not None
        return Path(
            pkg_resources.resource_filename(
                self.package,
                default_parquet_package_data_path(table_.name, data_dir),
            )
        )

    @output_handler(print_list)
    def dependent_tables(self, tables: Optional[Set[str]] = None) -> Set[str]:
        """Compute the set of tables downstream from a set of tables in the computational DAG,
        including the original tables"""
        tables = tables or set()
        unknown_tables = {t for t in tables if t not in self.schema.tables}
        if unknown_tables:
            raise KeyError(f"Unknown tables: {','.join(unknown_tables)}")
        dag = self.schema.dependency_dag()
        downstream = all_successors(dag, [self.schema.tables[t].graph_ref for t in tables])
        return {str(t) for t in downstream if isinstance(t, metaschema.ReferenceDataRef)}

    @output_handler(write_dependency_dag)
    def dag(
        self,
        tables: Optional[Set[str]] = None,
        *,
        upstream: bool = True,
        downstream: bool = True,
        build: bool = False,
    ):
        """Compute the dependency DAG for a set of tables and their dependencies and/or dependents.
        (or the whole DAG if tables are not passed)

        :param tables: tables to treat as root nodes in the DAG; if passed, only these tables and their
          dependencies/dependents will be in the DAG, otherwise the entire DAG will be returned
        :param upstream: Should the DAG include upstream dependencies? (by default it does)
        :param downstream: Should the DAG include downstream dependencies? (by default it does)
        :param build: Should the DAG include all dependencies that would be included in a build of the
          specified tables? Overrides upstream and downstream specification. False by default.
        :return: networkx.DiGraph representing the computational DAG of data derivations
        """
        if tables:
            if not upstream and not downstream and not build:
                raise ValueError("one of `upstream`, `downstream`, `connected` must be True")

            full_dag = self.schema.dependency_dag()
            table_refs = {self.schema.tables[t].graph_ref for t in tables}
            if build:
                tables_ = self.dependent_tables(tables)
                dag = self.schema.dependency_dag(lambda table: table.name in tables_)
            else:
                downstream_refs = all_successors(full_dag, table_refs) if downstream else set()
                upstream_refs = all_predecessors(full_dag, table_refs) if upstream else set()
                refs = downstream_refs.union(upstream_refs)
                dag = nx.DiGraph(nx.induced_subgraph(full_dag, refs))

            for table in table_refs:
                dag.add_node(table, initial=True)
            return dag
        else:
            return self.schema.dependency_dag()

    @output_handler(print_source)
    def compile(self, target: CompilationTarget) -> str:
        """Compile a schema YAML file to a specific target language/library

        :param target: The target language/library to compile the YAML schema to
        """
        if target == CompilationTarget.sqlite:

            def sql_renderer(schema):
                return "\n".join(render_sql_schema(schema))

            renderer = sql_renderer
        elif target == CompilationTarget.pandas:
            renderer = partial(
                render_pandera_module,
                package=self.package,
            )
        elif target == CompilationTarget.pyarrow:
            renderer = render_pyarrow_schema
        elif target == CompilationTarget.attrs:
            renderer = partial(
                render_attrs_module,
                package=self.package,
            )
        elif target == CompilationTarget.attrs_sqlite:
            assert (
                self.sqlite_db_path is not None
            ), "Must specify sqlite db path in build options to generate sqlite interface"
            renderer = partial(
                render_attrs_sqlite_schema,
                package=self.package,
                db_path=self.sqlite_db_path,
            )
        else:
            raise NotImplementedError(f"Compilation hasn't been implemented for target {target.value}")

        source = renderer(self.schema)
        return source

    @output_handler(print_file_hashes_status)
    def check_hashes(self) -> Dict[str, DataFileHashes]:
        """Check actual hashes of on-disk built data files against those documented in the schema"""
        assert (
            self.package_data_dir is not None and self.transient_data_dir is not None
        ), "Can't check hashes without package data dirs"
        hashes = {}
        for table in self.schema.build_time_package_tables:
            name = table.name
            loader = PandasParquetLoader.from_schema_table(
                table,
                package=self.package,
                data_dir=self.transient_data_dir if table.transient else self.package_data_dir,
            )
            if Path(pkg_resources.resource_filename(self.package, loader.data_path)).exists():
                hashes[name] = DataFileHashes(actual=loader.file_hash(), expected=table.md5)
            else:
                hashes[name] = DataFileHashes(actual=None, expected=table.md5)

        return hashes

    def init_sqlite(self, *, validate: bool = False, check_hash: bool = True):
        """Populate a sqlite database with the package's tabular data

        :param db_path: Optional path inside the package to use for the database. If not supplied, the
          default path will be used
        :param validate: Validate data using pandera schemas before inserting?
        :param check_hash: Check hashes in db metadata table and skip inserting tables that are
          up-to-date with current package data files?
        """
        assert (
            self.package_data_dir is not None
            and self.transient_data_dir is not None
            and self.sqlite_db_path is not None
        ), "Can't init sqlite db without package data dirs and sqlite db path"
        populate_sqlite_db(
            self.schema,
            db_package=self.package,
            db_path=self.sqlite_db_path,
            data_package=self.package,
            data_dir=self.package_data_dir,
            transient_data_dir=self.transient_data_dir,
            validate=validate,
            check_hash=check_hash,
        )

    def codegen(self):
        """Generate all derived accessor code and save to specified files"""
        self.build_command.write_derived_source_code()

    def docgen(self):
        if self.table_docs_dir is None:
            raise ValueError("Can't write table docs without table_docs_dir")
        elif self.type_docs_path is None:
            raise ValueError("Can't write type doc without type_docs_path")
        elif self.source_docs_path is None:
            self.logger.warning("Can't write source doc without source_docs_path")

        table_output_dir = Path(self.table_docs_dir)

        if table_output_dir.is_dir():
            self.logger.info(f"Clearing existing table docs directory at {table_output_dir}")
            shutil.rmtree(table_output_dir)

        self.logger.info(f"Creating table docs directory at {table_output_dir}")
        table_output_dir.mkdir(parents=True)

        self.logger.info("Rendering markdown for package tables")
        types_doc, source_doc, table_docs = render_sphinx_docs(
            self.schema, self.repo_root, self.repo_url
        )
        for table_name, markdown in table_docs.items():
            path = table_output_dir / f"{table_name}.rst"
            self.logger.info(f"Writing markdown docs for table {table_name} to {path}")
            with open(path, "w") as f:
                f.write(markdown)

        type_docs_path = Path(self.type_docs_path)
        self.logger.info(f"Writing markdown for package types to {type_docs_path}")
        with open(type_docs_path, "w") as f:
            f.write(types_doc)

        if self.source_docs_path:
            source_docs_path = Path(self.source_docs_path)
            self.logger.info(f"Writing markdown for package source data to {source_docs_path}")
            with open(source_docs_path, "w") as f:
                f.write(source_doc)

    def datagen(
        self, tables: Optional[Set[str]] = None, *, update_hashes: bool = True, no_sync: bool = False
    ):
        """Re-generate package data, optionally skipping files with hashes matching those in the schema
        :param tables: names of the specific tables to build. If not passed, all tables will be built
        :param update_hashes: Should hashes be updated for all tables regenerated at the end of the
          build? This is done by default but can be disabled if you are just experimenting.
        :param no_sync: when passed, don't pull the latest data from the remote blob store before building.
          Useful only if you really know what you're doing and are in an intermediate state with
          "uncommitted" data files whose md5s don't match what's in the schema - e.g. as a result of
          running `datagen` with `update_hashes=False`.
        """
        data_dir = self.package_data_dir
        transient_data_dir = self.transient_data_dir
        if data_dir is None or transient_data_dir is None:
            raise ValueError("Can't build data files without specification of data dirs in the schema")

        if tables:
            self.logger.info(
                f"Computing all tables downstream of {tables} in the dependency DAG and removing built "
                f"files to force re-computation"
            )
            # force re-computation of the specified tables *and* all their downstream dependents
            tables_to_recompute = self.dependent_tables(tables)
        else:
            # build all tables
            tables_to_recompute = set(t.name for t in self.schema.computable_tables)

        # update hashes for all upstream tables in the DAG as well, since any of them may be recomputed
        # in this build on a hash mismatch
        tables_to_update_hashes = {
            str(t)
            for t in self.schema.dependency_dag(lambda table: table.name in tables_to_recompute)
            if isinstance(t, metaschema.ReferenceDataRef)
            and not ((table := self.schema.tables[str(t)]).transient and table.md5 is None)
            # don't update hashes for transient tables with explicitly no hash
        }
        run_hash_update = bool(tables_to_update_hashes) and update_hashes

        if not no_sync:
            # ensure local blobs are up-to-date before building, but don't fail if a remote blob is absent;
            # we'll just regenerate it if it's needed for computing the current DAG
            self.sync_blob_store(down=True, no_fail_if_absent=True)

        for table_name in tables_to_recompute:
            table = self.schema.tables[table_name]
            file_path = self.data_path_for(table)
            if file_path.exists():
                self.logger.warning(f"Removing built file for table {table.name} at {file_path}")
                file_path.unlink()
            else:
                self.logger.info(f"No file found for table {table.name}; nothing to remove")
        try:
            self.build_command.build_package_data(tables=tables_to_recompute or None)
        except Exception as e:
            raise e
        finally:
            if run_hash_update:
                self.update_hashes(tables_to_update_hashes, codegen=True)

    def update_hashes(self, tables: Optional[Set[str]] = None, *, codegen: bool = True):
        """Update package data hashes in schema YAML to match the actual hashes of package data files as
        currently present in the file tree (or as recomputed when specified)

        :param tables: if passed, only update hashes for these tables' package data; otherwise update for
          all tables
        :param codegen: indicates whether to run the `codegen` command after updating the hashes to
          ensure hashes embedded in source code are up-to-date. By default, this runs when any hashes are
          updated in the config file.
        """
        assert (
            self.package_data_dir is not None and self.transient_data_dir is not None
        ), "Can't update hashes without package data dirs"
        hashes_updated = []
        tables_to_update = (
            [self.schema.tables[t] for t in tables] if tables else self.schema.build_time_package_tables
        )
        raw_schema = self.load_raw_schema()
        self.logger.info("Updating data hashes")
        for table in tables_to_update:
            table_name = table.name
            table_path = self.data_path_for(table)
            if table_path.exists():
                md5 = hash_file(table_path)
                old_md5 = table.md5
                if old_md5 is None:
                    self.logger.warning(
                        f"no md5 hash previously defined for table {table_name}; updating to {md5!r}"
                    )
                elif md5 != old_md5:
                    self.logger.warning(
                        f"md5 hashes did not match for table {table_name}; updating to {md5!r}"
                    )
                else:
                    continue

                table.md5 = md5
                raw_schema["tables"][table_name]["md5"] = md5
                hashes_updated.append(table_name)
            else:
                self.logger.warning(
                    f"package data file doesn't exist for table {table_name!r}; can't update md5 hash"
                )

        schema_path = pkg_resources.resource_filename(self.package, self.schema_path)
        if hashes_updated:
            self.logger.warning(
                f"updated hashes for tables {hashes_updated!r}; writing new schema to {schema_path}"
            )
            with open(schema_path, "w") as f:
                dump_yaml(raw_schema, f)

            if codegen:
                self.logger.info("regenerating source code to update embedded hashes")
                self.codegen()

    @noncommand
    def table_sync_data(self, table: metaschema.Table) -> TableSyncData:
        blob_store = self.schema.remote_blob_store
        assert blob_store is not None, "No blob store defined in schema"
        assert table.md5 is not None, f"No md5 defined for table {table.name}"
        assert self.package_data_dir is not None, "No package data dir to sync"
        local_build_path = Path(
            pkg_resources.resource_filename(
                self.package,
                default_parquet_package_data_path(table.name, self.package_data_dir),
            )
        )
        return TableSyncData(local_build_path, blob_store, md5=table.md5)

    @noncommand
    def sync_up(self, sync_data: TableSyncData) -> bool:
        remote_path = sync_data.remote_path
        local_build_path = sync_data.local_path
        if sync_data.remote_file_exists():
            self.logger.info(f"Found existing file in remote blob store at {remote_path}; not syncing")
            return True
        else:
            self.logger.info(f"Syncing to path {remote_path} in remote blob store")
            try:
                sync_data.remote_file_system.put_file(local_build_path, remote_path)
            except Exception as e:
                self.logger.exception(
                    f"Failed to put file {local_build_path} at {remote_path} in blob store: {e}"
                )
                return False
            else:
                return True

    @noncommand
    def sync_down(self, sync_data: TableSyncData, link_build: bool) -> bool:
        self.logger.info(f"Fetching file from remote blob store at {sync_data.remote_path}")
        try:
            paths = sync_adls_data(sync_data.remote_data_spec)
        except ADLSFileIntegrityError as e:
            self.logger.exception(str(e))
            return False
        except Exception as e:
            self.logger.exception(f"Failed to fetch file {sync_data.remote_path} from blob store: {e}")
            return False
        else:
            assert len(paths) == 1
            if link_build:
                local_cache_path = paths[0].local_path
                if sync_data.local_file_exists:
                    self.logger.warning(f"Removing existing file {sync_data.local_path}")
                    sync_data.local_path.unlink()
                self.logger.info(f"Linking downloaded file to local build file {sync_data.local_path}")
                sync_data.local_path.parent.mkdir(parents=True, exist_ok=True)
                link.link(local_cache_path, sync_data.local_path)
            return True

    def sync_blob_store(
        self,
        *,
        up: bool = False,
        down: bool = False,
        no_fail_if_absent: bool = False,
        tables: Optional[Set[str]] = None,
    ) -> List[str]:
        """Sync the local built files to the remote blob store, if one is defined.
        It is assumed that the hashes in the schema file are the source of truth rather than the hashes
        of the on-disk built files; if these should be taken as authoritative instead, run the
        `update_hashes` command first. At the end of this operation, all files in the local build folder
        and the remote blob store are guaranteed to match the hashes in the schema file, unless a file
        with the correct hash was unavailable.

        :param up: Upload local files to the blob store if they're available?
        :param down: Download remote blobs to the local build directory if they're available?
        :param no_fail_if_absent: when passed, don't fail an upload for lack of a local file being
          present with the expected hash for a version-controlled table. This is useful in development
          workflows where you just want to regenerate/sync a particular table that you've updated.
        :param tables: optional collection of table names to sync; all will be synced if not passed.
        :return: list of table names that were synced successfully
        :raises FileNotExistsError: if a local or remote file was not available for sync
        """
        assert self.package_data_dir is not None, "Can't sync blob store without package data dir"
        blob_store = self.schema.remote_blob_store
        if blob_store is None:
            self.logger.warning("No remote blob store defined; not syncing files")
            return []

        if not (down or up):
            raise ValueError("Must indicate syncing either down, up, or both from blob store")

        tables_to_sync = []
        for table in self.schema.build_time_package_tables:
            if table.md5 is None:
                self.logger.warning(
                    f"No md5 hash defined for package table {table.name}; no remote blob to sync to or from"
                )
            else:
                tables_to_sync.append(table)

        if tables is not None:
            known_tables = {t.name for t in tables_to_sync}
            if unknown_tables := tables.difference(known_tables):
                msg = f"Can't sync unknown or non-version-controlled tables: {', '.join(unknown_tables)}"
                self.logger.error(msg)
                raise KeyError(msg)
            tables_to_sync = [t for t in tables_to_sync if t.name in tables]

        self.logger.info(
            f"Syncing with remote blob store {blob_store.adls_account}/{blob_store.adls_filesystem}"
        )

        def inner(table: metaschema.Table) -> Optional[str]:
            sync_data = self.table_sync_data(table)
            local_file_md5 = sync_data.local_file_md5()
            if local_file_md5 == table.md5:
                # good local file; we can sync up
                self.logger.info(
                    f"Found local file for table {table.name} matching expected hash {table.md5}"
                )
                if up:
                    if self.sync_up(sync_data):
                        return table.name
                    else:
                        raise IOError(table.name)
                else:
                    # file is present locally with expected hash; no need to sync down
                    return table.name
            else:
                # check remote; download to get hash and link if good
                addendum = "" if local_file_md5 is None else f" matching expected hash {table.md5}"
                self.logger.info(f"No local file found for table {table.name}{addendum}")
                if up and no_fail_if_absent:
                    self.logger.info(
                        f"Skipping sync to remote blob store of local file for table {table.name}"
                    )
                    return None

                # only link the downloaded file into the build dir if we're syncing down; else just download
                # the file to check that it has the correct hash
                success = self.sync_down(sync_data, link_build=down)
                if success:
                    return table.name
                else:
                    if down and no_fail_if_absent:
                        return None
                    raise IOError(table.name)

        failed: list[tuple[str, Exception]] = []
        synced: List[str] = []
        for table_name, res in parallel.yield_all([(t.name, partial(inner, t)) for t in tables_to_sync]):
            if isinstance(res, parallel.Error):
                failed.append((table_name, res.error))
            elif res is not None:
                synced.append(table_name)

        if failed:
            first_exc = failed[0][1]
            table_names = [name for name, _ in failed]
            raise RuntimeError(f"Sync failed for tables {', '.join(table_names)}") from first_exc

        down_ = (
            f"to local build directory {pkg_resources.resource_filename(self.package, self.package_data_dir)}"
            if down
            else ""
        )
        up_ = (
            f"to remote blob store {blob_store.adls_account}/{blob_store.adls_filesystem}/{blob_store.path}"
            if up
            else ""
        )
        addendum = f"{down_} and {up_}" if down and up else down_ or up_
        tables_ = f" {', '.join(tables)}" if tables else ""
        self.logger.info(f"Success - build-time package data tables{tables_} synced {addendum}")
        return synced

    def pull(self, tables: Optional[Set[str]] = None, *, no_fail_if_absent: bool = False):
        """Download all remote blobs to the local data directory, with integrity checks.

        :param tables: optional collection of table names to sync; all will be synced if not passed.
        :param no_fail_if_absent: when passed, don't fail a download for lack of a remote blob being
          present in the bob store with the expected hash for a version-controlled table. This is useful
          in development workflows where you just want to regenerate/sync a particular table that you
          generated once and then removed, but didn't push yet (leaving a dangling hash reference).
        """
        self.sync_blob_store(down=True, no_fail_if_absent=no_fail_if_absent, tables=tables)

    def push(self, tables: Optional[Set[str]] = None, *, no_fail_if_absent: bool = False):
        """Upload all local data files to the remote blob store, with integrity checks.

        :param tables: optional collection of table names to sync; all will be synced if not passed.
        :param no_fail_if_absent: when passed, don't fail an upload for lack of a local file being
          present with the expected hash for a version-controlled table. This is useful in development
          workflows where you just want to regenerate/sync a particular table that you've updated.
        """
        self.sync_blob_store(up=True, no_fail_if_absent=no_fail_if_absent, tables=tables)

    @output_handler(print_schema_diff_summary)
    def schema_diff(
        self,
        base_ref: str = "HEAD",
        tables: Optional[Set[str]] = None,
        *,
        include_transient: bool = False,
        base_schema_path: Optional[str] = None,
    ):
        """Compute a diff between the current schema and a historical version of the schema.

        :param base_ref: the base git ref to compare against
        :param tables: a set of specific tables to inspect; if not passed the full schemas will be diffed
        :param include_transient: if passed, include transient tables in the analysis. These are usually
          implementation details of a derivation process and so are excluded by default (unless
          a transient table is specifically included in the `tables` argument)
        :param base_schema_path: path to the schema file to compare against; if not passed, the schema
          will be assumed to be present at the same location in the filesystem as the current schema.
          This enables loading of a historical schema even if the schema file or containing package have
          been moved or renamed.
        :return: a `SchemaDiff` object representing the differences between the two schemas
        """

        if base_schema_path is None:
            base_schema = load_schema(self.package, self.schema_path, git_ref=base_ref)
        else:
            base_schema = load_schema(None, base_schema_path, git_ref=base_ref)

        if tables is None and include_transient:
            this_schema = self.schema
        else:

            def table_pred(t: metaschema.Table) -> bool:
                return (include_transient or not t.transient) if tables is None else (t.name in tables)

            base_schema.tables = {name: t for name, t in base_schema.tables.items() if table_pred(t)}
            this_schema = copy(self.schema)
            this_schema.tables = {name: t for name, t in this_schema.tables.items() if table_pred(t)}

        return schema_diff.SchemaDiff(base_schema, this_schema)

    @output_handler(print_data_diff_summaries)
    def data_diff(
        self,
        base_ref: str = "HEAD",
        tables: Optional[Set[str]] = None,
        *,
        base_schema_path: Optional[str] = None,
        debug: bool = False,
    ) -> Iterator[Tuple[metaschema.Identifier, data_diff.DataFrameDiff]]:
        """Compute a diff between the current version-controlled data and the version-controlled data
        present at a historical point in time.

        :param base_ref: the base git ref to compare against
        :param tables: a set of specific tables to inspect; if not passed the full set of tables will be
          diffed
        :param base_schema_path: path to the schema file to compare against; if not passed, the schema
          will be assumed to be present at the same location in the filesystem as the current schema.
          This enables loading of a historical schema even if the schema file or containing package have
          been moved or renamed.
        :param debug: if True, pause execution at the first positive diff and drop into a debugger.
          The local `d_diff` object will be available in the debugger context.
        :return: an iterator of tuples of table names and their corresponding `DataFrameDiff`s. These
          may be consumed lazily, allowing for memory-efficient processing of large data diffs.
        """
        if tables:
            unknown = set(tables).difference(self.schema.tables.keys())
            if unknown:
                raise KeyError(f"Unknown tables: {', '.join(unknown)}")

        s_diff = self.schema_diff(base_ref, base_schema_path=base_schema_path)
        before_blob_store = s_diff.before.remote_blob_store
        after_blob_store = s_diff.after.remote_blob_store
        if before_blob_store is None or after_blob_store is None:
            raise ValueError("Can't diff data without remote blob stores defined in both schemas")
        for table_name, table_diff in sorted(s_diff.table_diffs.items(), key=lambda t: t[0]):
            if tables and table_name not in tables:
                continue
            if (not table_diff.before.md5) or (not table_diff.after.md5):
                if table_diff.after.build_time_installed and not table_diff.after.transient:
                    self.logger.warning(f"{table_name}: Can't diff without versioned data (md5 hashes)")
                continue
            if table_diff.before.md5 == table_diff.after.md5:
                self.logger.info(f"{table_name}: Matching md5 hashes; no data diff detected")
                continue

            if not (pkb := table_diff.before.primary_key) or not (pka := table_diff.after.primary_key):
                self.logger.warning(f"{table_name}: Can't diff without primary keys")
                continue
            if len(pka) != len(pkb):
                self.logger.warning(
                    f"{table_name}: Can't diff with different primary key lengths ({len(pkb)} vs {len(pka)})"
                )
                continue

            before_pk_cols = [next(c for c in table_diff.before.columns if c.name == k) for k in pkb]
            after_pk_cols = [next(c for c in table_diff.after.columns if c.name == k) for k in pka]
            incomparable = [
                (c1.name, c2.name)
                for c1, c2 in zip(before_pk_cols, after_pk_cols)
                if not parquet_util.pyarrow_type_compatible(
                    c1.type.parquet, c2.type.parquet, parquet_util.TypeCheckLevel.compatible
                )
            ]
            if incomparable:
                _incomparable = ", ".join(f"{a} <-> {b}" for a, b in incomparable)
                self.logger.warning(
                    f"{table_name}: Can't diff with incompatibly typed primary key columns {_incomparable}"
                )
                continue

            d_diff = data_diff.DataFrameDiff.from_tables(
                table_diff.before, table_diff.after, before_blob_store, after_blob_store
            )
            if debug and d_diff:
                breakpoint()
            yield table_name, d_diff


def main():
    if cli is None:
        raise RuntimeError(
            "CLI requirements not installed; include the 'cli' extra to use the tabularasa CLI"
        )

    cli.run()


if __name__ == "__main__":
    main()
