# thds.adls 4.0

## Design Summary

This was largely a performance optimization redesign.

The primary focus was addressing significant slowdowns in large file downloads that threatened to require
complex application-level workarounds (and that underpinned certain existing suboptimal designs that were
built around the constraints of slowness).

We confirmed that both disk and network throughput are constrained by node size, but also by specific
`azcopy` configuration.

## Details

### Historical Context and Failed Approaches

Previous optimization attempts had been made multiple times with limited success:

- `azcopy` testing showed only marginal improvements, suggesting potential network configuration issues
  or throttling between our infrastructure and `ADLS`
- Extensive investigation into disk and network throughput limitations had been inconclusive in past
  attempts

### Getting `azcopy` to work

- Despite extremely misleading information gathered using `lsblk -d -o NAME,SIZE,ROTA /dev/sda`, which
  indicated that we had a slow rotating disk, I was able to use
  `dd if=/dev/zero of=dd.test bs=1M count=204800 oflag=direct status=progress` to write a 200 GB file -
  much larger than available RAM - proving that disk throughput remained steady at 2.0 GB/s, which
  matches the theoretical disk throughput advertised by Azure for their 32 core nodes.
- I was able to confirm very fast downloads with `curl` and `wget`, and after much trial and error I was
  also able to get extremely fast downloads using `azcopy benchmark`.
- Nevertheless, it took a lot of extra investigation and trial and error to get `azcopy` to work well in
  an actual data download scenario; the confounding variables turned out to be `AZCOPY_CONCURRENCY` not
  being set inside our Python code, but even more so, our low value (0.3) of `AZCOPY_BUFFER_GB`, which
  had been set because of memory issues experienced on Kubernetes prior to our ability to dynamically
  identify our pod's actual available memory.
- Lastly, I discovered that `azcopy` was causing larger downloads to be throttled by its own internal MD5
  checking, which duplicated our own work. Disabling this with `--check-md5=NoCheck` resolved the
  remaining slowdown.

### Speeding up hashing

- Even with faster downloads, MD5 hashes are known to be very slow, and while we can cache those hashes
  on a local laptop, that's not usually very useful on a pod, where everything is being downloaded fresh
  every time.
- After looking for options, I settled on [`xxhash`](blake3/README.md), ~which hashes files in parallel
  and~ has a very optimized implementation that is trivially available with precompiled Python wheels for
  both Mac and Linux.
- Moving away from MD5 will incur some transition costs, and the refactor necessary was not small, but it
  enabled us to get rid of some legacy code (`AdlsHashedResource`) in favor of consistent use of
  `Source`, and over time the speed up should be well-worth it.

### Backward incompatible changes

I took this opportunity to remove some barely-used code that was heavily connected to the use of MD5. I
also cleaned up one or two interfaces and all their callers.

## Implications

- For workloads that need to do broadcast joins or very big downloads, prefer sharing that download
  across as many cores a possible (rather than splitting up your map job onto 60 pods on the same node).
- You may find a need to tweak `AZCOPY_CONCURRENCY` or `AZCOPY_BUFFER_GB` for your particular Docker
  image, or even inside a particular function.

## Next steps

- `upload` will need to support `azcopy` as well.
- We should not be using multiple simultaneous instances of `azcopy` as it's not really designed for
  that. We'll probably want to use `filelock` to coordinate across the entire machine.
