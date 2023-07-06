## 1.7.0

- `pure_remote` functions can now be called inside other `pure_remote`
  functions and they will transfer control via the runner rather than
  being called directly.  This would technically be a
  backward-incompatible change (although there are no current cases of
  this being used at Trilliant Health), so this behavior must be
  enabled via a flag on the `pure_remote` call itself. In a 2.0
  release, this behavior will be the default, and a method to disable
  it will be provided.

### 1.6.3

- Fix bug where `memoize_direct` got broken in 1.6.2 and apparently I
  had no tests on it? Not sure how this didn't break downstream
  applications but apparently it did not.

### 1.6.2

- Allow marking a function as remote-runnable when you're already
  inside a remote context.

  This is not generally recommended because it would be inefficient
  for one remote runner to call another remote runner and wait for it.

  However, there are cases where `memoize_direct` would be able to
  take advantage of this behavior at no efficiency loss, and the
  technical implementation is quite simple.

### 1.6.1

- Make `kubernetes` an optional dependency, via the `k8s` extra.

## 1.6

- Experimental API for ADLS Src/DestFiles, along with improved
  stability of serialization (and therefore improved memoization of
  functions using them). I am not yet committing to backward
  compatiblity for the things exported from `_src2` and `_dest2`, ,
  but it is reasonably likely this will serve as the basis for
  `mops 2.0` and the elimination of the current `adls_remote_files`
  implementation.

## 1.5

Minor features providing bootstrapping defaults for general use across
the monorepo.

- Add `pipeline_id_mask` decorator, such that libraries can set a
  default `pipeline_id` for an exported function that provides a known
  result, and so that applications can choose to override a library's
  `pipeline_id_mask` using the same decorator.
- `k8s_shell` will now accept a callable that returns the container
  name, so that this can be lazily deferred to the time of use.
- Add `std_docker_build_push_develop` helper to create a
  lazy-docker-image-building default approach to go along with the
  `k8s_shell` interface. This can be plugged in by applications to
  prevent accidentally running without rebuilding. See
  [`mldemo`](../../apps/mldemo/src/mldemo/k8s_choose_image.py#L13) for
  an example of this usage.

### 1.4.1

- Makes SrcFiles deterministically serializable in most cases, and
  always validates ADLS SrcFiles upon creation, even if they're
  remote. This is done in the service of greater reuse of memoized
  results.

## 1.4

Big changes to consistency and usability of memoization.

- Note that this release makes changes to AdlsPickleRunner internals
  as well as standard ADLS locations for function invocations and
  other blobs, so runs using previous versions of `mops` will not be
  able to take advantage of memoization across the upgrade, and Docker
  images for the remote workers executing `mops`-powered functions
  must be rebuilt. It remains backward-compatible with existing code.

- Upload shared blobs (Paths and Named objects) to
  content-hash-addressed locations in ADLS, in order to simplify data
  sharing and move toward greater possibility for trivial memoization.

- Calls to a PickleRunner-wrapped function can be configured as using
  memoized results from a previous run, such that identical arguments
  will result in retrieving those results even if the function has
  been moved or renamed, or a different pipeline id is being used for
  the rest of the run. This is configurable per-function as documented
  in the README.

- Embed base64-encoded `md5` checksum in serialized Src and DestFiles
  wherever we have access to that information. This will help us move
  toward more universal memoization, and would also uncover subtle
  bugs caused by files on ADLS being modified by some external
  process. MD5 is used because this is what Azure natively supports,
  and is acceptable because we are not using this for security
  purposes, but instead to avoid edge cases and improve user
  experience.


### 1.3.20230302160033

- Added custom Exception `BlobNotFoundError` to capture type hint, SA,
  container, and path when we are unable to fetch a blob from ADLS. We
  observed errors in Demand Forecast where blobs were missing, but it
  was difficult to understand what was going on since the only context
  we had were the native Azure SDK errors, which contain none of this
  information.

### 1.3.20230301002409

- Made a limit of `tempfile` name length in `remote.remote_file` of 150 characters.

### 1.3.20230227170917

- `ImageFileRef` makes sure to resolve Path on creation so that the
  semantics do not change after a later `os.chdir`.

### 1.3.20230222215430

- K8s Image Pull Backoff warnings will not die because of nil `last_timestamp` on event object.

### 1.3.20230220012057

- Fix image name inference for `std_find_image_full_tag` and add unit tests.

## 1.3

- Add tooling and CLI for launching arbitrary temporary/orchestrator pods from partial YAML.
- Add `krsync` wrapper to same CLI.

## 1.2

- Support Azure Workload Identity in known namespaces as provisioned
  by
  [Trilliant Health infrastructure](https://github.com/TrilliantHealth/engineering-infra/blob/main/engineering-stable/datascience/identities.tf#L4).
- `AdlsPickleRunner` now shares an underlying `FileSystemClient`
   across all parallel threads, leading to significant (5-7x) speedup
   when dealing with large (100+) numbers of parallel threads all
   trying to talk to the same storage account.
- `AdlsPickleRunner` can automatically re-run exceptions, assuming
  that intentional re-runs on the same pipeline id indicate that
  errors experienced by remote functions were transient. This behavior
  is configurable at the time of constructing the pickle runner, and
  is turned off by default.
- fix incorrect environment variable name `TRILLML_NO_K8S_LOGS` - is
  now `MOPS_NO_K8S_LOGS` as documented in the
  [README](src/thds/mops/k8s/README.md).
- Improvements to our handling of K8s API errors when attempting to
  watch lists of things (e.g. Jobs) that may change very rapidly.

## 1.1

- New `ImagePullBackOff` watcher utility available for integration with applications.
- Tiny file-based image-name-sharing abstraction intended for use in local development.
- Fix some odd bugs in `remote.remote_file` where `DestFiles` weren't correctly getting uploaded before exit.

# 1.0

Initial re-release as `mops`.

# Ancient History (`trilliant-ml-ops`) versions below:

## 30000003.7.0

- Optimistically fetch result or error from ADLS tmp path for each
  remote function execution even if the shell wrapper raises an
  Exception. This will work because the set of _actual_ failures is a
  strict subset of the set of _apparent_ failures, and actual failures
  can be reliably detected by the non-existence of any result/error
  payload at the intended path.

## 30000003.6.0

- Support building a shell dynamically based on the function being wrapped.

## 30000003.5.0

- Add new scripting utilities that wrap `docker build` for our applications.

## 30000003.4.0

- Add `inspect-pipenv` `pipenv` environment inspection tool for my own use.

## 30000003.3.0

- Automatically bump open file limits to reduce management load on users.

## 30000003.2.0

- Patch `joblib` batching to 10x performance with thousands of small tasks.
- Use K8s SDK Waiters to reduce load on K8s API by several orders of magnitude.
- Improve locked caching for AdlsPickleRunner.
- Improved logging for Job launch/completion.
- Fix additional issues with pipeline results reuse.

### 30000003.1.2

- Fix reuse of previous results with matching pipeline id.

### 30000003.1.1

 - Tolerate empty directories when listing in ADLS.
 - Fix logging message.
 - Fix export of AdlsDirectory.

## 30000003.1.0

- Cache pipeline root list results at the beginning of a remote
  pipeline function invocation to reduce excess ADLS usage.
- New `tempdir` helper that provides the intended semantics for
  returning a `pathlib.Path` from a remote function.
- New `YieldingMapWithLen` helper to provide serialization of Thunk
  creation in order to reduce peak memory pressure. Something that was
  useful for Demand Forecast but I suspect will be useful elsewhere
  also.
- Fix automatic namespace selection within PyCharm/VSCode (now
  correctly uses your username instead of `root`).
- Workaround for joblib memory view issue.
- Improvements for Azure credential usage that will hopefully reduce
  the amount of throttling we get from ADLS. The Azure Python SDK is a
  total disaster, but this is Python and we can fix anything!!

### 30000003.0.1

- Added some K8s toleration and labeling helpers.
- Only request MD5 of remote ADLS file if the local one is below a
  size threshold; this is mostly a hack to increase overall speed.

# 30000003.0.0

Changed stored format for various serialized objects, including pickled paths.

Added ability to have the AdlsPickleRunner do one-time pickling of specific named objects.

Provide a basic `joblib` backend that can use AdlsPickleRunner.

# 30000002.0.0

Changed stored format for ADLS-driven SrcFile and DestFile
(specifically, the uploader). This is a backwards-incompatible change,
so Docker images using this library must be rebuilt, and old cached
results will not be able to be loaded.
