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
control or remotely in ADLS (optionally pinned by an md5 hash to ensure build consistency), but are
packaged with the distribution as strictly-typed parquet files and optionally as a sqlite database
archive file. Large package files may be omitted from the base distribution to be synced with an ADLS
blob store at run time.

The sections of the schema file are as follows:

- `build_options`: a set of various flags controlling your build process, including code and data
  generation
- `tables`: the schema definitions of your tabular data, plus specifications of the inputs and functions
  used to derive them
- `types`: any custom constrained column-level types you may wish to define and reference in your tables.
  These become both validation constraints expressed as `pandera` schemas, and `typing.Literal` types in
  the case of enums, or sometimes `typing.NewType`s depending on your build options.
- `local_data`: specifications of local files in your repo that will be used to build your tables
- `remote_data`: specifications of remote files that will be used to build your tables. Currently only
  ADLS is supported.
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

When changes are made to a table in `schema.yaml`, either the schema or the source data, be sure to
update the associated derived package data file by running `tabularasa datagen <table-name>`. The hash
will then be updated to the new value either during this step or during pre-commit hook execution. See
the [package data generation section](#generating-package-data) for more information on this.

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

If you have just cloned the repo or pulled a branch and wish to generate all tables as they should be on
that branch, simply run

```bash
tabularasa datagen
```

which will regenerate all package data tables. By default this will skip regeneration of any tables whose
md5 hashes match those of any existing associated package data files on disk. Package data hashes in the
schema will then be updated for any package data that was generated.

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
`schema.yaml`, indicating the version of the data that should result from the build. If a file is already
present at build time, and the md5 sum of the file matches the one indicated in `schema.yaml`, then the
build step for that table can be skipped, saving you the wait.

To check the status of your local built data files with respect to the `schema.yaml` hashes, you can run

```bash
tabularasa check-hashes
```

To sync the hashes in `schema.yaml` with those of your generated data you can run

```bash
tabularasa update-hashes
```

By default this will also update your generated data accessor source code, which has the hashes embedded
in order to enable run-time integrity checks on fetch from the blob store, if you're using one.

#### Syncing with the Blob Store

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

If you're using a remote blob store for large files, you will want to include the invocation

```bash
tabularasa sync-blob-store --up
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

**NOTE**: This requires the `graphviz` source and binaries to be available on your system (`graphviz` is
a C library that doesn't come packaged with the python wrapper `pygraphviz`). The easiest way to ensure
this if you have a global anaconda env is to run `conda install graphviz`. However you proceed, you can
verify that `graphviz` is available by running `which dot` and verifying that a path to an executable for
the `dot` CLI is found (`dot` is one layout algorithm that comes with graphviz, and the one used in this
feature). Once you have that, you may `pip install pygraphviz` into your working dev environment. Refer
to the [pygraphviz docs](https://pygraphviz.github.io/documentation/stable/install.html) if you get
stuck.

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
