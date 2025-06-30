## 4.1

- Use `azcopy` for uploads over a certain size.

# 4.0

- Switch to `xxh3_128` hash for all new uploads, and prefer it for any download verification where it is
  embedded as the metadata key `hash_xxh3_128_b64`. This is a backward-incompatible change, since we no
  longer compute MD5 automatically for your uploads.
- Completely remove `AdlsHashedResource` - it was mostly obsoleted by `core.Source`, and is now
  completely obsolete since we are preferring a faster hash, `xxh3_128`.
- `adls.upload_through_cache` now returns a `core.Source` object rather than an `AdlsHashedResource`.
- rename `cached_up_down` to `cached`, to encourage namespace import/usage rather than the individual
  functions.

### 3.3.20250604

- Turns off azcopy globally by default

### 3.3.20250603

- Fix various places where we weren't returning connections to the underlying ConnectionPool, which led
  to a lot of verbose logging and potentially some slowdowns?

## 3.3

- `requires-python>=3.9`.

## 3.2

- Adds `adls.source_tree.from_path(<adls_uri_or_fqn>)` so that we can determinstically represent
  directories from ADLS.

## 3.1

- Adds the `copy`, and supporting `sas_tokens`, modules for copying files across ADLS storage locations.

# 3.0

- Remove deprecated `adls.link` alias for `core`.
- Add `named_roots` abstraction that decouples named ADLS storage locations from the `core.env` concept.

### 2.5.20241122

- Fix bug in the way we used a `scope.bound` on a generator that didn't always actually get consumed.

### 2.5.20241015

- Fix for change in how ADLS servers are interpreting length=0 when we didn't know the length of a byte
  stream.

## 2.5

```
- New `FastCachedAzureCliCredential`, which tries to avoid a lot of unnecessary runs of the Azure CLI for token acquisition on developer laptops, saving several seconds of runtime on many common tasks.
```

## 2.4

- Added `metadata` as an optional parameter for upload functions.

## 2.3

- Add global filelock to downloads so that multiple processes on the same machine do not try to download
  the same file in parallel.

## 2.2

- Adding a `UriIsh` union type and a `parse_any` function to the `uri` module.

### 2.1.20240208

- Fix longstanding bug where ResourceModifiedError could result during use of `resource.upload` because
  of a bug in the underlying `thds.core` library having to do with retries.

## 2.1

- Register handlers with `thds.core.source` so that `from_uri` will generate ADLS Source objects without
  hashes.
- Provide `thds.adls.source.from_adls`, which also supports `AdlsHashedResource` conversion into the more
  abstract and optimizable `Source` representation.

# 2.0

- Removing `lazy-object-proxy` from dependencies in favor of native lazy methods.
- `impl.adls_filesystem` is now `impl.make_adls_filesystem_getter` which lazily returns a closure.

## 1.7

- Add an environment variable `THDS_ADLS_UPLOAD_CHUNK_SIZE` (100MB by default) for more configurable
  uploads.

### 1.6.20230810

- Turns out a blob's ETag gets changed when its metadata is changed... which is what we've been doing at
  the end of downloads for files that have no known MD5. This change fixes the explosion that occurs when
  two simultaneous downloads using this same system both try to set the same MD5 on the same blob at the
  same time, and only one of them succeeds because the etag changes (and we conservatively don't change
  things if they've changed since we downloaded them).

## 1.6

- Set (missing) MD5s on all files downloaded via this library.
- All downloads will go into the cache now if one is provided. Previously, files with no known md5 would
  not have been downloaded there at all, but this was unnecessarily cautious, since files in the cache
  are not 'trusted' in any way.
- Disable symlinks to/from caches by default. They cause too many problems.
- Additionally, start putting the cache on the same volume as the repo checkout in GitHub runners. This
  will avoid the need for symlinks or copies, and won't require any more space, since hardlinks will be
  an option.

## 1.5

- Add `defaults` module that contains some simple env-based defaults for DS.

## 1.4

- Add write-through caching to `thds.adls.resource`.
- Provide `download_to_cache` and `upload_through_cache` shortcuts that are the simplest possible call
  when all you want is to upload or download some read-only bytes and you're sure you want to use the
  machine-global cache.
- Provide `resource.verify_or_create`, which checks a serialized resource pointer (a JSON file committed
  to the repository) and (re)creates the resource if that pointer cannot be found or the URI/FQN does not
  match what is expected. This is very handy for caching large test resources on ADLS and enabling them
  to be created transparently and non-imperatively, including in CI.

## 1.3

- Add a machine-global optimizing downloader that skips downloads based on local presence of the file and
  matching MD5. Additionally, add a linking cache implementation that can be inserted to optionally check
  a shared location for previous downloads. See `thds.adls.download` and `thds.adls.ro_cache`.

- `AdlsFqn` now supports the `.parent` property, which is the inverse of `join`, and behaves much like
  `pathlib.Path.parent`.

- New `abfss://` URI formatter for `AdlsFqn`.

## 1.2

- Automatically embed `MD5` sum in all files uploaded using the ADLSFileSystem utility. This also allows
  us to skip uploading a file if we detect that it's already been uploaded and has a matching checksum.
