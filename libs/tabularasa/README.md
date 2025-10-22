## Tabula Rasa

The `thds.tabularasa` package serves to enable version control, validation, and runtime access to tabular
datasets that are required in analytic and production workflows. As such, it encompasses a build system
for generating data, documentation for its derivation process, and code for accessing it.

### The Schema File

To use `tabularasa` in your project, you will first create a single yaml file defining a tabular schema
and build process. This file should exist within your package, not somewhere else in your repo - in other
words it should be package data. It is therefore always read and specified as any package data would be -
with a package name and a path inside said package.

The schema file includes documentation, tabular schema definitions, type information, value-level
constraints (e.g. ranges, string patterns, and nullability), column-level constraints (e.g. uniqueness),
file resource definitions, and build options controlling the output of the build system. Tables are built
from raw data files which may take any form and may be stored either in the repository under version
control or remotely in a blob store such as ADLS (versioned with md5 hashes to ensure build consistency),
but are packaged with the distribution as strictly-typed parquet files and optionally as a sqlite
database archive file. Large package files may be omitted from the base distribution to be synced with a
blob store at run time.

The sections of the schema file are as follows:

- `build_options`: a set of various flags controlling your build process, including code and data
  generation
- `tables`: the schema definitions of your tabular data, plus specifications of the inputs and functions
  used to derive them
- `types`: any custom constrained column-level types you may wish to define and reference in your tables.
  These become both validation constraints expressed as `pandera` schemas, and `typing.Literal` types in
  the case of enums, or sometimes `typing.NewType`s depending on your build options.
- `local_data`: specifications of local files in your repo that will be used to build your tables. Files
  referenced here are expected to be version-controlled along with your code and so don't require hashes
  for integrity checks.
- `remote_data`: specifications of remote files that will be used to build your tables. Currently only
  blob store backends like ADLS are supported. Files referenced here must be versioned with hashes to
  ensure build integrity (MD5 is used currently).
- `remote_blob_store`: optional location to store large artifacts in post-build, in case you want to set
  a size limit above which your data files will not be packaged with your distribution. They can then be
  fetched at run time as needed.
- `external_schemas`: optional specification of `tabularasa` schemas inside other packages, in case you
  are integrating with them, e.g. by sharing some types.

To get more detail on the structure of any of these sections, you may refer to the
`thds.tabularasa.schema.metaschema._RawSchema` class, which is an exact field-by-field reflection of the
schema yaml file (with a few enriched fields). Instances of this class are validated and enriched to
become instances of `thds.tabularasa.schema.metaschema.Schema`, which are then used in various build
operations.

### Core Concepts: How Tabularasa Controls Your Data

Before diving into the details, it's important to understand how tabularasa controls and transforms your
data:

#### Column Ordering

**Important**: The column order in your output parquet files is **entirely controlled by the order
defined in schema.yaml**, not by the order in your preprocessor code or source data. Even if your
preprocessor returns columns in a different order, tabularasa will reorder them to match the schema
definition during the build process. This ensures consistency across all data artifacts.

#### Primary Keys and Pandas Index

When working with pandas DataFrames, be aware that **primary key columns become the DataFrame index** and
effectively "disappear" from the regular columns. If you define `primary_key: [id, date]` in your schema,
those columns will be accessible via `df.index` rather than `df['id']` or `df['date']`. This behavior is
automatic and ensures efficient indexing for data access.

#### Transient Tables

Tables marked with `transient: true` are intermediate tables used during the build process but are not
included in the final package distribution. Use transient tables for:

- Raw input data that gets processed into final tables
- Intermediate transformation steps
- Large source data that shouldn't be shipped with the package

#### External Data Philosophy

Tabularasa follows a fundamental principle: **builds should never depend on external services**. All data
is snapshotted internally to ensure reproducible builds. This means:

- Data from external sources (APIs, remote CSVs, etc.) should be fetched and stored in version control or
  a blob store that you control (specified in the `remote_data` section)
- This ensures builds are deterministic and not affected by external service availability or consistency

### The Data Interfaces

The code generation portion of the build system can generate interfaces for loading the package parquet
data as `attrs` records or `pandas` dataframes (validated by `pandera` schemas), and for loading `attrs`
records from a `sqlite` archive via indexed queries on specific sets of fields.

The code for all modules is generated and written at [build time](#building).

### Building

To build your project with `tabularasa`, just run

```bash
tabularasa codegen
tabularasa datagen
```

from the project root, followed by the invocation of your standard build tool (`poetry`, `setuptools`,
etc).

This will generate all source code interfaces and package data according to various options specified in
the `build_options` section of the [schema file](#the-schema-file). Note that no code is written unless
the [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) of the generated python code differs from
what is found in the local source files. This allows the code generation step to avoid conflict with code
formatters such as `black`, since these change only the formatting and not the AST of the code.

### Adding new package data

To add a new table to the schema, place a new named entry under the `tables` section in your
[schema file](#the-schema-file). Source data for the table is specified in the table's `dependencies`
section. There are multiple ways to specify the source data, including version-controlled
repository-local files and remote files. Source data can be a standard CSV which can be translated
automatically into the table's typed schema, or some other data format that requires processing using a
user-defined function specified under a `preprocessor` key.

The simplest way to add new reference data to version control is to simply place a CSV in your repo, and
define the schema of that data in the `tables` section of your [schema file](#the-schema-file), pointing
the `dependencies.filename` of the table to the new CSV file.

#### Choosing Between Local and Remote Data

When deciding how to store your source data, consider these trade-offs:

**Local Data Storage Patterns**

Tabularasa supports two distinct patterns for managing local data files, each serving different
organizational needs. The **direct file reference pattern** allows tables to specify their data source
directly through `dependencies.filename`, providing a straightforward path to a file in the repository.
When you need to update the data, you simply overwrite the file and run `tabularasa datagen` without
making any schema changes. The framework reads the file directly using the provided path along with any
CSV parsing parameters specified in the dependencies block. This approach works best for data files that
are specific to a single table.

The **shared data pattern** using the `local_data` section provides a more structured approach for
managing data sources that multiple tables depend on. With this pattern, you define a named entry in the
`local_data` section of your schema that contains not just the filename but comprehensive metadata
including the data authority, source URL, update frequency, and documentation. Tables then reference
these entries using `dependencies.local: [entry_name]`. When the preprocessor function executes, it
receives a `LocalDataSpec` object that provides access to both the file (via the `full_path` property)
and all associated metadata. This pattern is best when multiple tables need to derive data from the same
source file, such as when several tables extract different subsets from a comprehensive dataset. This
centralized definition allows consistency across all dependent tables and makes it easier to track data
provenance and update schedules.

Both patterns store files in version control, making them ideal for datasets under 10MB that require
frequent updates. The key difference lies in organization and metadata management: direct references
prioritize simplicity and speed, while the local_data pattern emphasizes structure, reusability, and
documentation. Larger files should use remote storage instead.

**Remote Data Storage in Blob Store**

Remote data storage through a blob store (e.g., ADLS) addresses the scalability limitations of local file
storage. When source datasets exceed 10MB, the `remote_data` section of the schema file allows you to
reference files stored in a blob store. Each remote data entry specifies paths to files in the blob store
along with their MD5 hashes to ensure the correct version is downloaded during builds. While this
approach keeps the repository lean, it requires a more structured workflow: you must upload source files
to the blob store, calculate their MD5 hashes, and specify them in the schema. This additional complexity
makes remote storage most suitable for stable, infrequently changing source datasets where the overhead
of managing source file hashes is justified by the benefits of centralized storage and repository size
optimization.

Note that MD5 hash management differs by context: source files in `remote_data` require manual MD5 hash
specification, while tables that generate parquet files have their MD5 hashes automatically calculated
and updated by `tabularasa datagen`. Local source files referenced through `local_data` or
`dependencies.filename` do not require MD5 hashes since they are assumed to be versioned by your version
control system.

Example workflow for monthly updates with local data:

```yaml
# schema.yaml - Direct file reference pattern
tables:
  my_monthly_data:
    dependencies:
      filename: build_data/monthly_data.csv  # Fixed filename
      # Monthly: Download new CSV → overwrite file → datagen
```

Example of shared local_data pattern:

```yaml
# schema.yaml - Shared data pattern
local_data:
  census_data:  # Define once
    filename: build_data/census_2023.xlsx
    url: https://census.gov/data/...
    authority: US Census Bureau
    last_updated: 2023-07-01
    update_frequency: Yearly

tables:
  state_demographics:
    dependencies:
      local: [census_data]  # Reference from multiple tables
  county_statistics:
    dependencies:
      local: [census_data]  # Same source, consistent metadata
```

Example workflow for remote data:

```yaml
# schema.yaml
remote_data:
  my_large_data:
    paths:
    - name: data/large_file_2024_01.parquet
      md5: abc123...  # Must update this hash for each new version
```

When changes are made to a table in `schema.yaml`, either the schema or the source data, be sure to
update the associated derived package data file by running `tabularasa datagen <table-name>`. The table's
MD5 hash will then be automatically updated to reflect the new generated parquet file either during this
step or during pre-commit hook execution. See the
[package data generation section](#generating-package-data) for more information on this.

To understand all the ways of defining a table or file dependency, take a look at the schema file data
model defined in the `thds.tabularasa.schema.metaschema._RawSchema` class. This represents an exact
field-by-field reflection of the contents of the schema yaml file.

### The CLI

When installed, the `thds.tabularasa` package comes with a CLI, invoked as `tabularasa` or
`python -m thds.tabularasa`. In the examples that follow, we use the `tabularasa` invocation. This CLI
supplies various utils for development tasks like building and fetching data, generating code and docs,
and checking package data integrity.

Each of these functionalities can be invoked via

```
tabularasa <subcommand-name>
```

for the subcommand that accomplishes the intended task.

The CLI can be made more verbose by repeating the `-v` flag as many times as necessary just after
`tabularasa` and before the name of the subcommand being invoked. If you should want them, the CLI can
self-install its own set of bash-compatible completions by running
`tabularasa --install-bash-completions`.

Documentation for the main CLI or any subcommand can be accessed in the standard way with `--help`:

```bash
tabularasa --help  # main CLI args and subcommand list
tabularasa <command-name> --help  # help for command identified by <command-name> - its purpose and args
```

The CLI is by default configured by a config file (JSON or YAML) in the working directory called
`tabularasa.yaml`. This just supplies a few required pieces of information, namely the name of the
`package` that you're interacting with and the `schema_path` relative to the package root, so that you
don't have to pass them as options on the command line. Most other important information relevant to the
CLI operations is contained in the [schema file](#the-schema-file) itself, especially the `build_options`
section.

To use the CLI in another project as a build tool, you will need to specify `thds.tabularasa[cli]` as
your dependency. The `cli` extra comes with some dependencies that are only needed in the context of the
CLI which are somewhat heavy and so best left out of your environment if you don't explicitly need them.

Of course if you need the CLI as a development dependency but you only need the _library_ at run time,
you may specify just `thds.tabularasa` as your main dependency and `thds.tabularasa[cli]` as your dev
dependency.

Some useful subcommands of the CLI are documented below.

#### Generating package data

If you're adding new tables or updating the data in a set of tables, especially when using a custom
preprocessor, you will likely want to repeatedly regenerate the package data parquet files for those
tables in order to confirm that the build is working as intended.

To do so, run

```bash
tabularasa datagen <table-name-1> <table-name-2> ...
```

All of the tables you specify _and_ all of their dependents downstream in the computational DAG will thus
be re-computed. This saves you from the work of keeping track of the downstream dependents, a tedious and
error-prone task. It ensures that all your package data and associated hashes are up to date, which
finally ensures that your peers will have up-to-date data when they get a cache miss after pulling your
code changes.

Any derived table upstream of those you request to build with `datagen` will be auto-synced from the blob
store prior to the build running, if available, saving you the wait time of re-building them needlessly
in case they're not already in your working tree.

If you'd like to better understand what you changed after any `tabularasa datagen` invocation before you
commit the result, you can run `tabularasa data-diff`. By default, this diffs the data as versioned in
the working tree against the data as versioned in the HEAD commit. If you've already committed, you can
pass a ref to the previous commit, e.g. `tabularasa data-diff HEAD~`. This will show summary stats
describing the changes, such as the number of rows added, removed, and modified for each updated table.
With the `--verbose` flag added, you can see more detail, for instance the row counts for each row-level
pattern of updates (e.g. in 10 rows, columns 'A' and 'B' were updated, in 5 rows, column 'C' was nulled,
in 3 rows, column 'A' was filled, etc.).

If you wish to regenerate _all_ package data tables from scratch, you can run

```bash
tabularasa datagen
```

This will remove all pre-existing package data files and re-generate them. This is an extreme measure and
should be used sparingly; in most cases, you will want to only those specific tables whose source data or
derivation logic you know has changed.

Note that if you have just cloned the repo or pulled a branch and wish to get your local package data
up-to-date with the state on that branch, you don't need to re-derive all the data! Just
[sync with the blob store](#syncing-with-the-blob-store) instead.

#### Inspecting auto-generated code

If you'd like to review the code changes that would result from any change to the schema or compilation
modules without over-writing the existing generated source (as a [build](#building) could do), there is a
simple CLI command for inspecting it.

To inspect e.g. the auto-generated pandas code for the current repo state, run

```bash
tabularasa compile pandas
```

The code will print to stdout. Simply replace `pandas` with `attrs`, `sqlite`, `attrs-sqlite`, or
`pyarrow` to see the code generated for those use cases.

#### Checking integrity of local built reference data

The build pipeline uses md5 hashes to prevent expensive re-builds in local runs. When the
[build](#building) finishes, you will have several parquet files and possibly a sqlite database archive
present in your file tree. Each of the parquet files should have an associated md5 checksum in
`schema.yaml`, indicating the version of the data that should result from the build.

To check the status of your local built data files with respect to the `schema.yaml` hashes, you can run

```bash
tabularasa check-hashes
```

**Important**: The following shouldn't be required in normal usage: use with care and only if you know
what you're doing!

To sync the hashes in `schema.yaml` with those of your generated data you can run

```bash
tabularasa update-hashes
```

By default this will also update your generated data accessor source code, which has the hashes embedded
in order to enable run-time integrity checks on fetch from the blob store, if you're using one. In
general, you _should not need to to this manually_ however, since `tabularasa datagen` will update the
hashes for you as part of its normal operation.

#### Syncing with the Blob Store

**Important**: The `push`, `pull`, and `sync-blob-store` commands work **only with final parquet
tables**, not with input source data. Input data (specified in `local_data` or `remote_data`) is only
accessed during `datagen` execution.

Under the section `remote_blob_store` in [the schema file](#the-schema-file), you may optionally specify
a remote cloud storage location where built package data artifacts are stored. In case
`build_options.package_data_file_size_limit` is set, the package in question will not come with any
package data files exceeding that limit in size. These _will_ be available in the remote blob store, and
in case they are not present when one of the [data loaders](#the-data-interfaces) is invoked, will be
downloaded into the package.

Should your use case require the data to be locally available at run time, e.g. if you lack connectivity,
then you may fetch all the package data tables that were omitted in the [build](#building) by running

```bash
tabularasa sync-blob-store --down
```

or just

```bash
tabularasa pull
```

If you're using a remote blob store for large files, you will want to include the invocation

```bash
tabularasa sync-blob-store --up
```

or just

```bash
tabularasa push
```

somewhere in your CI build scripts after the [build](#building) completes and before you publish your
package, to ensure that those files are available at run time to end users when needed.

#### Initializing the SQLite Database

To initialize the SQLite database (see [interfaces](#the-data-interfaces)), should one be needed but not
shipped as package data (as specified in the `build_options` section of
[the schema file](#the-schema-file)), you may run

```bash
tabularasa init-sqlite
```

This will create the SQLite database archive in your installed package directory. For an added level of
safety you may pass `--validate` (to validate the inserted data against the constraints defined in
[the schema file](#the-schema-file) as expressed as [pandera schemas](#the-data-interfaces)), but these
will usually be statically verified once at build time and guaranteed correct before shipping.

#### Visualizing the Data Dependency DAG

The `dag` command creates a graph visualization of your project's dependency DAG and subsets thereof. The
visualization is opened in a browser (it's SVG by default) but if you pass `--format png` for example it
will open in an image viewer.

To visualize your data dependency DAG, from your project root run

```bash
tabularasa dag                  # generate full DAG
tabularasa dag [table-name(s)]  # generate DAG for specific tables
```

> [!NOTE]
> This requires the `graphviz` source and binaries to be available on your system (`graphviz` is a C
> library that doesn't come packaged with the python wrapper `pygraphviz`). The easiest way to ensure
> this if you have a global anaconda env is to run `conda install graphviz`. However you proceed, you can
> verify that `graphviz` is available by running `which dot` and verifying that a path to an executable
> for the `dot` CLI is found (`dot` is one layout algorithm that comes with graphviz, and the one used in
> this feature). Once you have that, you may `pip install pygraphviz` into your working dev environment.
> Refer to the [pygraphviz docs](https://pygraphviz.github.io/documentation/stable/install.html) if you
> get stuck.

## Generating documentation

To generate the documentation for your project, run:

```bash
tabularasa docgen
```

from your project root.

This generates docs in ReStructuredText (rst) format in a directory structure specified in the
`table_docs_path`, `type_docs_path`, and `source_docs_path` fields of the
[schema file](#the-schema-file)'s `build_options` section. As such, these docs are valid as input to the
`sphinx` documentation build tool.

## Memory usage

Your reference data may be fairly large, and in multiprocessing contexts it can be useful to share the
read-only data in memory between processes for the sake of performance.

`tabularasa` builds this in via mem-mapped SQLite for the most part, but the default Python installation
of SQLite [limits](https://www.sqlite.org/mmap.html) the amount of memory-mapped data to 2GB per database
file.

A project called `pysqlite3` packages the same shim code alongside the ability to provide a different
shared library for SQLite, and their built binary package
[increases](https://github.com/coleifer/pysqlite3/blob/master/setup.py?ts=4#L107) the memory cap to 1TB.
Currently, the precompiled package is only available for Linux.

The good news: if you want more reference data to be shared between processes, all you need to do is
successfully install a version of `pysqlite3` into your Python environment. If you're on Linux, likely
you can accomplish this with a simple `pip install pysqlite3-binary`. On a Mac, you'll need to follow
their [instructions](https://github.com/coleifer/pysqlite3#building-with-system-sqlite) for linking
against a system-installed SQLite, or build against a statically-linked library and then install from
source.

If `pysqlite3` is installed in your Python environment, it will be used within `tabularasa` by default.
To disable this behavior, set the `REF_D_DISABLE_PYSQLITE3` environment variable to a non-empty string
value.

By default, with `pysqlite3` installed, 8 GB of RAM will be memory-mapped per database file. With the
standard `sqlite3` module, the limit will be hard-capped at 2 GB. If you want to change this default, you
can set the `REF_D_DEFAULT_MMAP_BYTES` environment variable to an integer number of bytes.
