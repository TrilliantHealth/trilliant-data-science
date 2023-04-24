# Src/Dest Files

This is the one interface that `mops` introduces that will
specifically change the code that you are using with `mops`. It is
designed to be low-impact, but nevertheless requires you to opt in to
a particular method of getting things done.

 - _The primary use case here is large amounts of data being generated
[remotely](./remote.md), and you want to be able to point future
processes and/or remote runtimes at that data without being forced to
download the data to the [orchestrator](./orchestrator.md)._

If you're looking for something that doesn't impact your code, and
you're willing to download the data on the orchestrator before passing
it anywhere else, take a look at the `pathlib.Path` [optimization
documented here](./optimizations.md).

> ADLS is the only currently-supported remote filesystem for `SrcFile`
> and `DestFile`.

However, both of these abstractions are specifically designed to allow
you to use them even if you're not running remotely - they will
transparently use the local filesystem instead of ADLS with no
performance penalty.

## DestFile

The `DestFile` specifies a path relative to both a known remote root
and the local application working directory at which a function should
place a file result of its computation. Upon return from a remote
runtime to the local orchestrator process, a small, JSON, "remote file
pointer" will be placed on local storage at a corresponding local
path.

> You _must_ return the DestFile from the decorated function.  If it
> is not part of the result payload, it will _NOT_ be uploaded and the
> data will be LOST.

`DestFile` supports two methods of creation:

1. On the local orchestrator process, before passing to the
   remote. Use `AdlsDatasetContext.dest`, which will specify a
   location for the remote file pointer to be placed upon return from
   the remote process. Prefer this usage, because it makes clearer the
   _purity_ of your function, since the destination of the final data
   was a parameter to the function and therefore cannot accidentally
   be non-deterministic.
2. Less preferably, it may be created on the remote process (inside
   the decorated function). Use `AdlsDatasetContext.remote_dest`. The
   data will be uploaded to that ADLS path, and if returned to the
   local orchestrator, the local filepath will be a concatenation of
   the process working directory and the full ADLS path. If you're not
   careful to pick the remote path determinstically based on the
   parameters to your function, this will make your function
   ['logically impure'](./pure_functions.md).

The populated `DestFile` can then be 'converted' on the local
orchestrator (either from the remote pointer on the local filesystem,
or the `DestFile` object already in memory) into a `SrcFile` and
passed directly to future remote functions, where, when accessed via
its Context Manager, the backing remote file will be downloaded and
made available for read-only access.

As an example of this flow, you might do something like the following:

```python
from thds.mops.remote import DestFile, SrcFile, adls_dataset_context, pure_remote, ...

def orchestrator(...):
    my_ds = adls_dataset_context('my-dataset')
    created_dest = remote_creator(my_ds.dest('relative/path/where/i/want/it.parquet'))
    # created_dest now actually exists on your filesystem, but only as a pointer
    result_dest = remote_processor(
        my_ds.dest('relative/path/to/final/result.parquet'),
        my_ds.src(created_dest),
    )
    # result_dest also exists on your filesystem as a pointer.

@pure_remote(...)
def remote_creator(dest: DestFile, *args, **kwargs) -> DestFile:
    created_file_path = create_stuff(*args, **kwargs)
    with dest as dest_path:
        # when this context closes, the file at dest_path will be uploaded as necessary
        created_file_path.rename(dest_path)
        return dest  # dest must be returned in order to be referenced in the orchestrator

@pure_remote(...)
def remote_processor(dest: DestFile, src: SrcFile, *args, **kwargs) -> DestFile:
    with src as src_path:
        # this makes sure the src path is available locally
        result_df = process_stuff(src_path, *args, **kwargs)
        with dest as dest_path:
            result_df.to_parquet(dest_path)
            return dest_path
```

The Context Managers are a bit ugly, so you may wish to use
`thds.core.scope.enter` to avoid the extra nesting.

⚠️  However, it is critical that your `DestFile` context be closed
_before_ exiting the `pure_remote`-decorated function, or else your
data will remain in the temp location and will not get delivered to
its final destination.

## SrcFile

`SrcFile` actually supports three different methods of creation,
depending on your situation.

1. Locally present file that you want uploaded for a given process run
   if and only if the function actually gets run remotely. Use
   `AdlsDatasetContext.src`. A local-only run will transparently use
   the local file.
2. Locally present remote file pointer (JSON string), created using
   `DestFile` or with some other means. Use `AdlsDatasetContext.src`
   for this as well. A local-only run will have to download the file
   inside the function where it is used.
3. Fully remote file that you have never downloaded, with no
   locally-present remote file pointer. Use
   `AdlsDatasetContext.remote_src`. A local-only run will have to
   download the file.

Any use of a remote-only `SrcFile` will require that it be downloaded
 upon every access, even if computing locally (skipping the
 `pure_remote` decorator). At the present time, there is no fancy
 caching that happens on a per-process basis. The `SrcFile` Context
 Manager will be forced to re-download the file after every `__exit__`
 and subsequent `__enter__`. This is an implementation detail and
 could potentially be lifted in the future.
