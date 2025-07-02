# some notes on blake3 hashing, which turned out to be complicated

## deadlocks with blake3.update_mmap and blake3.update

This is non-deterministic and did not happen in most usage, but in a few cases where we ended up trying
to `blake3().update_mmap(a_file)` in a process and it deadlocks.

It's not limited to `update_mmap` - it happens with `update` itself.

I do not have strong evidence about exactly what deadlocks, but here's what I know:

- This behavior can happen whether there are multiple processes trying to hash the same file at roughly
  the same time, or only one.
- there's no CPU usage from any of the involved processes, and overall system usage is low.
- the Rayon (Rust) threads are not getting launched
- system memory usage is also very low
- I can launch a _separate_ Python process and use the exact same Python wrapping code to hash the exact
  same file... and it succeeds just fine! So it's not a lock on the file, exactly...
- I have waited up to an hour for the problem to resolve itself, and it never does. It hangs
  indefinitely.
- This only (so far) happens when one process has hashed a file using blake3 and then created child
  processes that start via `fork`. If I use `multiprocessing.set_start_method('spawn')`, the deadlock
  does not occur.

In any case, this is a coordination problem. We _could_ try to refactor our applications to not _need_
the download locks, but this will introduce complexity.

## benchmarking results (or, this is why we always want to use update_mmap)

- [32 core in k8s](./benchmark_32_core.md)
- [M1 MacBook Pro](./benchmark_mac.md)

## Rejected fixes

### Stop downloading in parallel

this sounds like a nice idea but would have far-reaching consequences for all users of `thds.adls` and
other shared code. I do not want people to have to be 'aware' of these things in order to have a
functioning program.

### Don't use threads

My hunch is that part of our problem here is using threads with blake3. I haven't actually tested that,
but it might be that blake3 with no threads would never run into this issue. However, I _have_ tested it
without threads in the general case, and it is _dramatically_ slower. It's not worth the slowdown, which
takes it back to looking more like MD5.

### use a daemon process responsible for performing blake3 hashes

This was my plan for a while. It _would_ work. But the code complexity necessary to keep it from becoming
a performance bottleneck for small files is quite big. And the irony is that doing complex
multiprocessing 'things' is a great way to _introduce_ deadlocks.

## Planned fix

### Use `xxhash`

`blake3` is fully parallelized and nothing else that I've been able to find is.

But I tested xxhash on my local machine, and it is actually _faster_ than blake3 on very large inputs,
which I think has to do with blake3 running out of RAM and moving into swap. This is an actual concern
for us, too, because we don't want Kubernetes processes to run out of memory, since no swap is available
to them at all.

On my laptop, I can hash a 50GB file in about 11 seconds with xxhash xxh3 128. blake3 takes 44s to hash
the same file, and uses all the memory i have available.

On Kubernetes, `blake3` is definitely faster - 1.6 seconds, whereas xxhash still takes about 10 seconds.
I expect `blake3` could be even faster on a 64 core node... though obviously there will be diminishing
returns here.

Although that performance difference is dramatic, shaving 8 seconds off our runtimes for the very largest
files is not worth the deadlock complexity, and it's debatable whether it would have been worth the risk
of OOM errors on smaller pods dealing with large files.

```python
def nocache_xxhash_file(file: StrOrPath) -> str:
    import xxhash

    fh = xxhash.xxh3_128()
    return hashing.b64(hashing.hash_using(file, fh).digest())


def nocache_blake3_file(file: StrOrPath) -> str:
    bh = blake3(max_threads=blake3.AUTO)
    return hashing.b64(bh.update_mmap(file).digest())
```
