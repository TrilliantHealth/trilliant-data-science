## 1.4

- Add write-through caching to `thds.adls.resource`.
- Provide `download_to_cache` and `upload_through_cache` shortcuts
  that are the simplest possible call when all you want is to upload
  or download some read-only bytes and you're sure you want to use the
  machine-global cache.

## 1.3

- Add a machine-global optimizing downloader that skips downloads
  based on local presence of the file and matching MD5. Additionally,
  add a linking cache implementation that can be inserted to
  optionally check a shared location for previous downloads. See
  `thds.adls.download` and `thds.adls.ro_cache`.

- `AdlsFqn` now supports the `.parent` property, which is the inverse
  of `join`, and behaves much like `pathlib.Path.parent`.

- New `abfss://` URI formatter for `AdlsFqn`.

## 1.2

- Automatically embed `MD5` sum in all files uploaded using the
  ADLSFileSystem utility. This also allows us to skip uploading a file
  if we detect that it's already been uploaded and has a matching
  checksum.
