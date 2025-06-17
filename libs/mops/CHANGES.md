### 3.8.20250602

- Now raising an error instead of silently overwriting when duplicate basenames are used as output
  `Source`s. See [docs](docs/optimizations.adoc#thds.core.source) for details.

### 3.8.20250529

- Restore previous values for log watching timeouts in `mops.k8s`.

### 3.8.20250516

- Removed stack-local context that will not mask nested pipeline ids defined in the various ways they may
  be desired to function.

### 3.8.20250425

- Fix incompatibility with Python 3.12+ because of backward-incompatible change made by
  `importlib.metadata` in Python 3.12.

## 3.8

- Adds `calls` API to `pure.magic` and `MemoizingPicklingRunner`, which serves as a way to have inner
  `function-logic-key`s invalidate memoization for functions which are known to call them.

## 3.7

- `requires-python>=3.9`.

### 3.6.20250409

- Fixes a bug in Kubernetes Job name generation when no user prefix was supplied.

### 3.6.20250328

- Fixes a bug where `mops.pure.magic` config would not be correctly loaded when it was a `__mask` config.

## 3.6

- [New `pure.magic` API for `mops`](docs/magic.adoc). A collection of lessons learned and long wished-for
  bits of developer-friendliness.
- `BlobStore` implementations can now be registered dynamically, or as a `thds.mops.pure.blob_stores`
  entrypoint (via `importlib.metadata`). As before, a BlobStore is chosen by matching a URI.
- `mops` summary files will be output in the `.mops/summary` directory, to make room for other usage of
  that directory in the future. Technically this is a 'breaking' change, but in practice it will make no
  difference to current users.
- Renaming the `Shell/Builder` concept to `Shim/Builder`, to distance ourselves from other technical
  concepts that people commonly associate with the word shell.

## 3.5

- Never completely clear the cache of previously-observed K8s Objects during `watch`. Instead, manually
  track the times of updates we receive from the API, both overall and per object. Previously seen
  objects can now eventually go stale, but we'll see fewer false 'disappearances' when the K8s API is
  under heavy load.
- Improve a few internal names and an exception message.

### 3.4.20241204

- Fix bug where a `Source` object created using `from_file(a_path, uri='adls://...')` did not properly
  force an upload inside the mops machinery during return from a remote invocation.

### 3.4.20241126

- Fix long-standing bug in how we interpreted the Kubernetes Job status object, occasionally leading to
  false positive 'Job failed' errors. Apparently this has been
  [a point of confusion](https://github.com/kubernetes/kubernetes/issues/68712#issuecomment-499716681) in
  the Kubernetes community as well, which makes me feel slightly better.

## 3.4

- All `source.Source` objects that can be found in return values are added to the output mops run summary
  files, so that `mops-summarize` can print them for you, and so that you can, e.g., write your own `jq`
  scripts to somehow process every Source object and its URI.

## 3.3

- Can log a fraction of the Kubernetes pods launched. Non-determinstic.

## 3.2

- Improve lock/lease behavior so that the remote runners will exit early if they detect an intervening
  acquirer. This will prevent rare (but observed) scenarios wherein dying orchestrators, relinquishing
  their leases while their remote is still pending scheduling on a cluster somewhere, end up leading to
  multiple parallel invocations that race to completion, potentially overwriting each others' results in
  a manner that can lead to unstable memoization akin to a merge conflict.

## 3.1

- Supports automatically discovering `function-logic-key` in first-class function objects passed as
  arguments to a wrapped function. In other words, `function-logic-key` is no longer only for the
  top-level function, but can be used to annotate anything that is an argument (direct or indirect) to
  the top-level function. Note that `mops` still cannot discover which functions your (non-argument)
  functions call, as this is ultimately akin to the halting problem.

# 3.0

- BREAKING: Completely remove SrcFile and DestFile and all associated code.
- Embed metadata in `result` and `exception` payloads, including who invoked the function, when, and what
  version of the code was run. Track some metadata from the original invocation.
- Embed returned metadata into the mops summary files.
- Report on some of the metadata via the `mops-summarize` tool, including average runtimes for the actual
  invocations.

## 2.13

- Forward-compatibility shims for upcoming mops 3.0.
- content-type is now added to the mops control files when uploaded to ADLS.

### 2.12.20241015

- Fix bug where we relied on a semantic version being part of the package, which was guaranteed only by a
  specific upstream part of `thds.core`, and was a bad tradeoff.

### 2.12.20241009

- We had previously begun to allow you to specify a `service_account_name` for your `mops.k8s`-launched
  Kubernetes image - but we did not directly apply the magic incanation that would allow you to make use
  of our `orchestrator` service account and its intended cluster role. This release fixes that issue by
  automagically applying the correct role (and binding).

## 2.12

- `pure.require_all_results` now supports a message to be presented to the user in case the results do
  not exist. Additionally, it supports providing an environment variable which can be set to disable the
  check for that particular case. The message, if provided, and a note about the option to override with
  environment variable, will be included in the exception that is raised.

### 2.11.20241001

- Fix bug in `image.py` where we wouldn't build the upstream Docker image (if configured) in its own
  virtual environment.

### 2.11.20240927

- Fix bug where, when not finding required results, we only uploaded the `invocation` and did not do the
  deferred work upon which that invocation relied to be intelligible.

### 2.11.20240906

- Fix bug where `deferred_work` made mops itself not re-entrant within a threadlocal_shell, because the
  same deferred work context for the parent call was still open when the child call was run remotely.

### 2.11.20240905

- Fix bug where `threadlocal_shell` (`memoize_in`) was not performing deferred work by moving that
  deferred work to where it really belonged, in the MemoizingPicklingRunner, immediately prior to calling
  the shell.
- Reduce the surface area for confusion around mops bugs by not transmitting remote-side exceptions back
  to the orchestrator when it was actually mops code that failed somehow.

## 2.11

- Optimize Path and Source uploads, so that in cases of memoized results (very common!), we do not need
  to talk to Azure/ADLS _at all_ until it's time to check to see if there is already a result. And if
  there is, then we completely skip writing hash refs and uploading Paths (which obviously must have
  already been uploaded in the past if there's a result). Even though those uploads would in most cases
  have been optimized by `thds.adls` to a check of existing, matching bytes, that check is plenty slow
  enough to be noticed by a human user, and it's quite silly to bother when the memoized result is about
  to be discovered.
- Use a local cache for control files (mainly, `result` files) so that we can look up results without
  needing to make a network hop. This cuts out the last perceptible effects of `mops` on retrieval of
  known results.

## 2.10

- Factored `mops._utils.human_b64` and the module's corresponding tests out into a new library `humenc`,
  which `mops` now depends on.

## 2.9

- `mops` now coordinates pending function invocations globally - in other words, it has become a
  (rudimentary) DAG runner that requires no code changes to opt into.

## 2.8

- `image.default_config` will now always attempt a local image build if a remote build is attempted and
  does not happen because the repository is dirty.

### 2.7.20240617

- Fix a bug where the old Azure Managed Identity (`aadpodidbinding=ds-standard`) was incorrectly not
  being enabled for users not on The List.

## 2.7

- Introduced a `mops-summarize` CLI tool to generate function usage summary reports for pipeline runs.
  The summarizer can aggregate logs from a given pipeline run directory and generate a concise report,
  including function execution details and memoized functions cache hits.

## 2.6

- `ImageFileRef` (and the `default_config` constructor for it) now default to ensuring that there is an
  up-to-date Docker image built for your `k8s`-enabled `mops` functions. It even builds the Docker image
  remotely, so that you don't have to use `docker` or push large layers to Azure Container Registry.

## 2.5

- Support new hash-based `Source` representation from `thds.core`, intended to replace both Paths and
  SrcFile/DestFile for future representation of read-only data across remote function boundaries in
  `mops`. Fully optimized to avoid unnecessary uploads (unlike `Path`), requires no temp files (unlike
  `SrcFile`), and avoids ceremony associated with context managers (unlike both `SrcFile` and
  `DestFile`).

### 2.4.20241217

- Fixed a bug where a username including capital letters or underscores would result in an error when the
  Kubernetes namespace was chosen.
- Refactored the `mops.config` system to use `thds.core.config`.

## 2.4

- Invocation-derived output AdlsFqns have changed to retain file extensions so they'll be more
  user-friendly when downloaded. This is not backward-incompatible in general, but is an observable
  change in existing behavior.
- New `pure.adls.rdest` `DestFile` creator that builds on top of `invocation_output_fqn` with some "do
  what I mean" behavior. This is in a working state but is not guaranteed to provide maximal
  forward-memoizability (its serialized representations or automatic ADLS location determination could
  change).
- `src_from_dest` no longer forces upload if there was no Runner involved.
- `pipeline-id-mask` in function docstrings will now get applied automatically as long as no other mask
  has been defined. This reduces the boilerplate necessary to use the pipeline id mask concept.
- You can globally register handlers that can modify the
  [pipeline memospace](docs/memoization.adoc#memospace-parts) for a programatically-derived subset of
  functions. See `pure.add_pipeline_memospace_handlers` and the provided default implementation,
  `pure.matching_mask_pipeline_id`, which is meant to allow you to override/mask the pipeline id using
  regexes that `re.match` (not `fullmatch`) the fully qualified module path for your functions, e.g.
  `thds.myapp.thing_a`.
- Fixed a bug where `SrcFile` would get serialized improperly the second time it was serialized within a
  given application, because some of its local-only state was not properly being excluded from the
  serialization.

## 2.3

- `KeyedLocalRunner` provides a memoizing-only interface that acts like a memo-key-selector over top of
  `MemoizingPicklingRunner`. See [Advanced Memoization](docs/advanced_memoization.adoc) for details.

## 2.2

- `invocation_output_fqn` now uses the active storage root (corresponding to the memoization root) by
  default, rather than requiring an argument.
- (Not sure if this version was ever actually released as 2.2...)

### 2.1.20230920

- Enable memoization of functions inside the module named `__main__` when the function is called in the
  same process as the original orchestrator. This enables writing simple one-file scripts that make use
  of memoization alone.

## 2.1

- Unique names for functions from `mops.pure.core.memo.unique_name_for_function` now separates module
  from function name with "--" instead of ":", because the presence of the latter in URIs can cause
  issues for Spark read/writes.

# 2.0

Cleaning up a lot of old cruft. No big fancy new features.

Many renames.

- `mops.remote` was renamed to `mops.pure`.
- `@pure_remote` decorator factory is now `@use_runner`.
- `mops.k8s.image_ref` is now `mops.image`.
- `mops.remote.remote_file` is now `mops.srcdest`.
- `mops.remote.adls_remote_files` is now `mops.pure.adls`.

## 1.8.0

- `MemoizingPickledFunctionRunner` now supports lazy `storage_root` configuration.
- Exceptions are now pickled using `tblib`, so the orchestrator-side stack trace should look a lot more
  meaningful.

## 1.7.0

- `pure_remote` functions can now be called inside other `pure_remote` functions and they will transfer
  control via the runner rather than being called directly. This would technically be a
  backward-incompatible change (although there are no current cases of this being used at Trilliant
  Health), so this behavior must be enabled via a flag on the `pure_remote` call itself. In a 2.0
  release, this behavior will be the default, and a method to disable it will be provided.

### 1.6.3

- Fix bug where `memoize_in` got broken in 1.6.2 and apparently I had no tests on it? Not sure how this
  didn't break downstream applications but apparently it did not.

### 1.6.2

- Allow marking a function as remote-runnable when you're already inside a remote context.

  This is not generally recommended because it would be inefficient for one remote runner to call another
  remote runner and wait for it.

  However, there are cases where `memoize_in` would be able to take advantage of this behavior at no
  efficiency loss, and the technical implementation is quite simple.

### 1.6.1

- Make `kubernetes` an optional dependency, via the `k8s` extra.

## 1.6

- Experimental API for ADLS Src/DestFiles, along with improved stability of serialization (and therefore
  improved memoization of functions using them). I am not yet committing to backward compatiblity for the
  things exported from `_src2` and `_dest2`, , but it is reasonably likely this will serve as the basis
  for `mops 2.0` and the elimination of the current `adls_remote_files` implementation.

## 1.5

Minor features providing bootstrapping defaults for general use across the monorepo.

- Add `pipeline_id_mask` decorator, such that libraries can set a default `pipeline_id` for an exported
  function that provides a known result, and so that applications can choose to override a library's
  `pipeline_id_mask` using the same decorator.
- `k8s_shell` will now accept a callable that returns the container name, so that this can be lazily
  deferred to the time of use.
- Add `std_docker_build_push_develop` helper to create a lazy-docker-image-building default approach to
  go along with the `k8s_shell` interface. This can be plugged in by applications to prevent accidentally
  running without rebuilding. See [`mldemo`](../../apps/mldemo/src/mldemo/k8s_choose_image.py#L13) for an
  example of this usage.

### 1.4.1

- Makes SrcFiles deterministically serializable in most cases, and always validates ADLS SrcFiles upon
  creation, even if they're remote. This is done in the service of greater reuse of memoized results.

## 1.4

Big changes to consistency and usability of memoization.

- Note that this release makes changes to AdlsPickleRunner internals as well as standard ADLS locations
  for function invocations and other blobs, so runs using previous versions of `mops` will not be able to
  take advantage of memoization across the upgrade, and Docker images for the remote workers executing
  `mops`-powered functions must be rebuilt. It remains backward-compatible with existing code.

- Upload shared blobs (Paths and Named objects) to content-hash-addressed locations in ADLS, in order to
  simplify data sharing and move toward greater possibility for trivial memoization.

- Calls to a PickleRunner-wrapped function can be configured as using memoized results from a previous
  run, such that identical arguments will result in retrieving those results even if the function has
  been moved or renamed, or a different pipeline id is being used for the rest of the run. This is
  configurable per-function as documented in the README.

- Embed base64-encoded `md5` checksum in serialized Src and DestFiles wherever we have access to that
  information. This will help us move toward more universal memoization, and would also uncover subtle
  bugs caused by files on ADLS being modified by some external process. MD5 is used because this is what
  Azure natively supports, and is acceptable because we are not using this for security purposes, but
  instead to avoid edge cases and improve user experience.

### 1.3.20230302160033

- Added custom Exception `BlobNotFoundError` to capture type hint, SA, container, and path when we are
  unable to fetch a blob from ADLS. We observed errors in Demand Forecast where blobs were missing, but
  it was difficult to understand what was going on since the only context we had were the native Azure
  SDK errors, which contain none of this information.

### 1.3.20230301002409

- Made a limit of `tempfile` name length in `remote.remote_file` of 150 characters.

### 1.3.20230227170917

- `ImageFileRef` makes sure to resolve Path on creation so that the semantics do not change after a later
  `os.chdir`.

### 1.3.20230222215430

- K8s Image Pull Backoff warnings will not die because of nil `last_timestamp` on event object.

### 1.3.20230220012057

- Fix image name inference for `std_find_image_full_tag` and add unit tests.

## 1.3

- Add tooling and CLI for launching arbitrary temporary/orchestrator pods from partial YAML.
- Add `krsync` wrapper to same CLI.

## 1.2

- Support Azure Workload Identity in known namespaces as provisioned by
  [Trilliant Health infrastructure](https://github.com/TrilliantHealth/engineering-infra/blob/main/engineering-stable/datascience/identities.tf#L4).
- `AdlsPickleRunner` now shares an underlying `FileSystemClient` across all parallel threads, leading to
  significant (5-7x) speedup when dealing with large (100+) numbers of parallel threads all trying to
  talk to the same storage account.
- `AdlsPickleRunner` can automatically re-run exceptions, assuming that intentional re-runs on the same
  pipeline id indicate that errors experienced by remote functions were transient. This behavior is
  configurable at the time of constructing the pickle runner, and is turned off by default.
- fix incorrect environment variable name `TRILLML_NO_K8S_LOGS` - is now `MOPS_NO_K8S_LOGS` as documented
  in the [README](src/thds/mops/k8s/README.md).
- Improvements to our handling of K8s API errors when attempting to watch lists of things (e.g. Jobs)
  that may change very rapidly.

## 1.1

- New `ImagePullBackOff` watcher utility available for integration with applications.
- Tiny file-based image-name-sharing abstraction intended for use in local development.
- Fix some odd bugs in `remote.remote_file` where `DestFiles` weren't correctly getting uploaded before
  exit.

# 1.0

Initial re-release as `mops`.

# Ancient History (`trilliant-ml-ops`) versions below:

## 30000003.7.0

- Optimistically fetch result or error from ADLS tmp path for each remote function execution even if the
  shell wrapper raises an Exception. This will work because the set of _actual_ failures is a strict
  subset of the set of _apparent_ failures, and actual failures can be reliably detected by the
  non-existence of any result/error payload at the intended path.

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

- Cache pipeline root list results at the beginning of a remote pipeline function invocation to reduce
  excess ADLS usage.
- New `tempdir` helper that provides the intended semantics for returning a `pathlib.Path` from a remote
  function.
- New `YieldingMapWithLen` helper to provide serialization of Thunk creation in order to reduce peak
  memory pressure. Something that was useful for Demand Forecast but I suspect will be useful elsewhere
  also.
- Fix automatic namespace selection within PyCharm/VSCode (now correctly uses your username instead of
  `root`).
- Workaround for joblib memory view issue.
- Improvements for Azure credential usage that will hopefully reduce the amount of throttling we get from
  ADLS. The Azure Python SDK is a total disaster, but this is Python and we can fix anything!!

### 30000003.0.1

- Added some K8s toleration and labeling helpers.
- Only request MD5 of remote ADLS file if the local one is below a size threshold; this is mostly a hack
  to increase overall speed.

# 30000003.0.0

Changed stored format for various serialized objects, including pickled paths.

Added ability to have the AdlsPickleRunner do one-time pickling of specific named objects.

Provide a basic `joblib` backend that can use AdlsPickleRunner.

# 30000002.0.0

Changed stored format for ADLS-driven SrcFile and DestFile (specifically, the uploader). This is a
backwards-incompatible change, so Docker images using this library must be rebuilt, and old cached
results will not be able to be loaded.
