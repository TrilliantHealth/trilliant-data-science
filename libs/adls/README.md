# thds.adls

A high-performance Azure Data Lake Storage (ADLS Gen2) client for the THDS monorepo. It wraps the Azure
SDK with hash-aware caching, azcopy acceleration, and shared client/credential plumbing so applications
can transfer large blob datasets quickly and reliably.

## Highlights

- **Environment-aware paths first:** Almost every consumer starts by importing `fqn`, `AdlsFqn`, and
  `defaults.env_root()` to build storage-account/container URIs that follow the current THDS environment.
- **Cache-backed reads:** `download_to_cache` is the standard entry point for pulling blobs down with a
  verified hash so local workflows, tests, and pipelines can operate on read-only copies.
- **Bulk filesystem helpers:** `ADLSFileSystem` powers scripts and jobs that need to walk directories,
  fetch batches of files, or mirror hive tables without re-implementing Azure SDK plumbing.
- **Spark/Databricks bridges:** `abfss` and `uri` conversions keep analytics code agnostic to whether it
  needs an `adls://`, `abfss://`, `https://`, or `dbfs://` view of the same path.
- **Composable utilities:** Higher-level modules (cache, upload, copy, list) layer on top of those
  imports so teams can opt into more advanced behavior without leaving the public API surface.

## Key Modules

| Component                             | Typical usage in the monorepo                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `fqn`                                 | Parse, validate, and join ADLS paths; used when materializing model datasets and configuring pipelines.    |
| `AdlsFqn`                             | Strongly typed value passed between tasks and tests to represent a single blob or directory.               |
| `defaults` / `named_roots`            | Resolve environment-specific storage roots (`defaults.env_root()`, `named_roots.require(...)`).            |
| `download_to_cache` (`cached` module) | Bring a blob down to the shared read-only cache before analytics, feature builds, or test fixtures run.    |
| `ADLSFileSystem` (`impl` module)      | Fetch or list entire directory trees and integrate with caching inside scripts and notebooks.              |
| `abfss`                               | Translate `AdlsFqn` objects into `abfss://` URIs for Spark/Databricks jobs.                                |
| `uri`                                 | Normalize `adls://`, `abfss://`, `https://`, and `dbfs://` strings into `AdlsFqn` values (and vice versa). |
| `global_client` / `shared_credential` | Shared, fork-safe Azure clients and credentials backing the public helpers above.                          |

## Getting Started

1. Ensure repo toolchains are installed (`mise trust`, `uv sync` from `libs/adls/` if you need an
   isolated environment).

1. Import the library via `from thds.adls import ...`. The package auto-registers hash algorithms with
   `thds.core` on import.

1. Configure named roots at application startup:

   ```python
   from thds.adls import named_roots, defaults, uri

   named_roots.add(prod=uri.parse_uri("adls://storageAcct/container/"))
   default_root = defaults.env_root()  # respects THDS_ENV by default
   ```

1. Use the caching helpers and Source integration:

   ```python
   from thds.adls import cached, upload, source

   cache_path = cached.download_to_cache("adls://acct/container/path/to/file")
   src = upload("adls://acct/container/path/out.parquet", cache_path)
   verified = source.get_with_hash(src.uri)
   ```

1. For CLI usage, run (from repo root):

   ```bash
   uv run python -m thds.adls.tools.download adls://acct/container/path/file
   ```

## Operational Notes

- **Hash metadata:** Uploads attach `hash_xxh3_128_b64` automatically when the bytes are known. Download
  completion back-fills missing hashes when permissions allow.
- **Locks and concurrency:** Large transfers acquire per-path file locks to keep azcopy instances
  cooperative. Global HTTP connection pools default to 100 but are configurable via `thds.core.config`.
- **Error handling:** `BlobNotFoundError` and other ADLS-specific exceptions translate into custom error
  types to simplify retries and diagnostics.
- **Extensibility:** Additional hash algorithms can be registered by importing dependent packages (e.g.,
  `blake3`). Named roots can be populated dynamically via environment-specific modules
  (`thds.adls._thds_defaults` hook).

## Testing & Tooling

- Unit tests live under `libs/adls/tests/`; run with `uv run pytest`.
- For integration/bulk validation, prefer `mono validate` from repo root.
- When touching I/O logic, follow repo logging guidance (log paths, filters, row counts, key dtypes).

______________________________________________________________________

Feel free to adapt this reference for onboarding docs or PRs. Contributions that improve coverage,
performance, or docs are welcome—keep diffs focused and reversible.
