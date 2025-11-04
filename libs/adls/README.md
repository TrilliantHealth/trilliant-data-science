# thds.adls

A high-performance Azure Data Lake Storage (ADLS Gen2) client for the THDS monorepo. It wraps the Azure
SDK with hash-aware caching, azcopy acceleration, and shared client/credential plumbing so applications
can transfer large blob datasets quickly and reliably.

## Highlights

- **Unified URIs & roots:** `adls://`, `https://`, `abfss://`, and `dbfs://` inputs resolve to canonical
  `AdlsFqn` objects. Named roots and `defaults.env_root()` let apps avoid hardcoding
  storage-account/container pairs.
- **Hash-first workflows:** Registers `xxh3_128` (and optionally `blake3`) with `thds.core`, embeds
  hashes into blob metadata on upload, and verifies hashes before and after download.
- **Read-only cache:** A global, length-safe cache directory stores verified downloads and optionally
  write-through uploads. Helpers hard-/soft-link or copy cache entries into local paths while keeping
  cached bytes protected.
- **azcopy acceleration:** Large transfers default to azcopy with tuned flags (`--check-md5=NoCheck`,
  custom buffer/concurrency). Smaller files fall back to the Azure SDK automatically.
- **Resilient downloads:** `download_or_use_verified` coordinates hash checks, download locks, retries,
  and metadata back-fill through a coroutine controller shared by sync and async call sites.
- **Upload symmetry:** `upload()` (and azcopy helpers) skip redundant uploads when hashes match, write
  through the cache, and return fully-hashed `thds.core.Source` objects.
- **Parallel listing & copying:** `list_fast` parallelizes blob enumeration, `source_tree` builds
  hash-aware manifests, and `copy` functions move data between ADLS locations with SAS delegation and
  optional completion waits.
- **Shared clients & credentials:** Global factories widen HTTP connection pools, stay fork-safe, and use
  thread-safe Azure credentials. Local macOS installs benefit from cached Azure CLI tokens; pods use
  workload-identity-friendly defaults.
- **CLI tools:** `uv run python -m thds.adls.tools.{download,upload,ls,ls_fast}` provide quick manual
  entry points for common operations.

## Key Modules

| Module                                                  | Purpose                                                                               |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `fqn`, `uri`, `abfss`, `dbfs`                           | Parse and format ADLS identifiers, scheme adapters, named roots.                      |
| `cached`, `ro_cache`, `download_lock`                   | Read-only cache management, download throttling, directory mirroring.                 |
| `download`, `azcopy.download`                           | Hash-verified downloads with azcopy fast path, SDK fallback, metadata reconciliation. |
| `upload`, `azcopy.upload`, `_upload`                    | Hash-aware uploads, azcopy command builder/runner, retry logic for race-prone blobs.  |
| `hashes`, `md5`, `etag`                                 | Hash registration, metadata extraction, compatibility utilities.                      |
| `impl.ADLSFileSystem`                                   | Async Azure SDK wrapper for file/directory fetches and cache-aware operations.        |
| `global_client`, `shared_credential`, `_fork_protector` | Fork-safe cached client factories and credential shims.                               |
| `source`, `source_tree`, `list_fast`                    | Integration with `thds.core.Source` abstractions and fast blob manifest generation.   |
| `copy`, `sas_tokens`                                    | Remote-to-remote data movement via SAS delegation and hash comparison.                |
| `tools/*`                                               | Command-line entry points for download/upload/list workflows.                         |

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
   src = upload.upload("adls://acct/container/path/out.parquet", cache_path)
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
