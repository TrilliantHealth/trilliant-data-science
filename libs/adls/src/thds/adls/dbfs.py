import typing as ty

from .fqn import AdlsFqn, AdlsRoot, join, parse_fqn

DBFS_SCHEME = "dbfs:/"

ADLS_TO_SPARK_MAPPING = {
    "adls://uaapdatascience/data/": "/mnt/datascience/data/",
    "adls://thdsdatasets/prod-datasets/": "/mnt/datascience/datasets/",
    "adls://uaapdatascience/hive/": "/mnt/datascience/hive/",
    "adls://thdsscratch/tmp/": "/mnt/datascience/scratch/",
}
ADLS_TO_DBFS_MAPPING = {k: join(DBFS_SCHEME, v) for k, v in ADLS_TO_SPARK_MAPPING.items()}
# Spark read/write implicitly adds a 'dbfs:/' prefix.
SPARK_TO_ADLS_MAPPING = {v: k for k, v in ADLS_TO_SPARK_MAPPING.items()}
DBFS_TO_ADLS_MAPPING = {join(DBFS_SCHEME, k): v for k, v in SPARK_TO_ADLS_MAPPING.items()}


def to_adls_root(root_uri: str) -> AdlsRoot:
    try:
        return AdlsRoot.parse(
            DBFS_TO_ADLS_MAPPING[root_uri]
            if root_uri.startswith(DBFS_SCHEME)
            else SPARK_TO_ADLS_MAPPING[root_uri]
        )
    except KeyError:
        raise ValueError(f"URI '{root_uri}' does not have a defined ADLS root!")


def to_adls_fqn(fully_qualified_name: str) -> AdlsFqn:
    mapping = (
        DBFS_TO_ADLS_MAPPING if fully_qualified_name.startswith(DBFS_SCHEME) else SPARK_TO_ADLS_MAPPING
    )

    try:
        dbfs_root, adls_root = next(
            ((k, v) for k, v in mapping.items() if fully_qualified_name.startswith(k))
        )
    except StopIteration:
        raise ValueError(f"{fully_qualified_name} does not have a defined ADLS path!")

    return parse_fqn(join(adls_root, fully_qualified_name.split(dbfs_root)[1]))


def to_uri(adls_path: ty.Union[AdlsRoot, AdlsFqn], spark: bool = True) -> str:
    def get_root_uri(adls_root: AdlsRoot) -> str:
        try:
            return (
                ADLS_TO_SPARK_MAPPING[str(adls_root)] if spark else ADLS_TO_DBFS_MAPPING[str(adls_root)]
            )
        except KeyError:
            raise ValueError(f"{str(adls_root)} does not have a corresponding dbfs root!")

    if isinstance(adls_path, AdlsRoot):
        return get_root_uri(adls_path)

    try:
        return join(get_root_uri(adls_path.root()), adls_path.path)
    except ValueError:
        raise ValueError(f"{str(adls_path)} does not have a corresponding dbfs path!")
