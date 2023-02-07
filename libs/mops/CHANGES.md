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
