# Src/Dest Files

> These are used for special cases and most of the time you should
> probably just `from thds.adls import download_to_cache,
> upload_through_cache` alongside
> `thds.mops.pure.adls.invocation_output_fqn(name='foobar')`.

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

An ADLS-based `DestFile` may be created on the local orchestrator or
inside the remote, but you should strongly prefer to find some method
of guaranteeing that the underlying fully-qualified name (FQN) for your
DestFile is unique to your invocation and correctly reflects the intended
ADLS Storage Account and Container that is configured at the time of running
the orchestrator process.

In general, this means passing all or at least the root of a storage
URI/FQN into the function as an argument. It's strongly recommended
that you not construct a URI from scratch on the remote function, as
your application config will likely not carry over to the remote, and
this will cause your function to be [impure](./pure_functions.md).

A good general pattern to use is to construct the DestFile on the
remote, but to construct a URI based on `invocation_output_fqn`, like
so: ```
destf = adls.dest(invocation_output_fqn(storage_root_passed_to_function, name='a-meaningful-name'))
```

The populated `DestFile` can then be 'converted' on the local
orchestrator (either from the remote pointer on the local filesystem,
or the `DestFile` object already in memory) into a `SrcFile` and
passed directly to future remote functions, where, when accessed via
its Context Manager, the backing remote file will be downloaded and
made available for read-only access.

As an example of this flow, you might do something like the following:

```python
from thds.adls import defaults
from thds.mops import srcdest


def orchestrator(...):
    my_app_root = defaults.env_root() / 'myapp'
    mk_destfile = srcdest.DestFileContext(my_app_root, 'a_local_dir')
	mk_srcfile = srcdest.SrcFileContext(my_app_root)

    created_dest = remote_creator(mk_destfile('relative/path/where/i/want/it.parquet'))
    # created_dest now actually exists on your filesystem, but only as a pointer

    result_dest = remote_processor(
        mk_destfile('relative/path/to/final/result.parquet'),
        mk_srcfile(created_dest),
    )
    # result_dest also exists on your filesystem as a pointer.

@use_runner(...)
def remote_creator(dest: srcdest.DestFile, *args, **kwargs) -> srcdest.DestFile:
    created_file_path = create_stuff(*args, **kwargs)
    with dest as dest_path:
        # when this context closes, the file at dest_path will be uploaded as necessary
        created_file_path.rename(dest_path)
        return dest  # dest must be returned in order to be referenced in the orchestrator

@use_runner(...)
def remote_processor(dest: srcdest.DestFile, src: srcdest.SrcFile, *args, **kwargs) -> srcdest.DestFile:
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
_before_ exiting the `use_runner`-decorated function, or else your
data will remain in the temp location and will not get delivered to
its final destination.

## SrcFile

`SrcFile` actually supports three different methods of creation,
depending on your situation.

1. Locally present file that you want uploaded for a given process run
   if and only if the function actually gets run remotely. Use
   `srcdest.local_src` or `srcdest.SrcFileContext`. A local-only run will
   transparently use the local file.
2. Locally present remote file pointer (JSON string), created using
   `DestFile` or with some other means. Use `srcdest.load_srcfile` for
   this.  A local-only run will have to download the file inside the
   function where it is used.
3. Fully remote file that you have never downloaded, with no
   locally-present remote file pointer. Use `srcdest.src` or
   `srcdest.SrcFileContext`. A local-only run will have to download
   the file.

In theory, any use of a remote-only `SrcFile` will require that it be
 downloaded upon every access, even if computing locally (skipping the
 `use_runner` decorator). However, for ADLS SrcFiles, the underlying
 `thds.adls` download implementation will use a global cache for the
 download.
