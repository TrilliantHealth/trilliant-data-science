## Limitations

### ADLS

This is the only blob store we currently support. In the future, we
could lift this implementation, as `mops` is already designed in such
a way as to minimize the direct dependency on ADLS.

### No 'remote recursion'

Currently, only a single 'level' of remote functions are
supported. That is to say, once running remotely, no function which is
itself a remote function may be called and expected to be launched on
yet another remote machine.

This is a simple technical limitation and may be removed with some
additional work.

### Network bottlenecks

This library has improved over time, but over many runs we have
observed that while you can run small-to-medium workloads against ADLS
just fine, large workloads (tens of thousands of `pure_remote` calls
simultaneously) may be difficult to manage over a network connection
between your laptop and the Azure datacenters, causing spurious
authentication, connection timeout, and other network errors. If you
start running into these issues you can running your orchestrator
process on the 'inside' of an Azure datacenter on a
[pod inside](./kubernetes.md#orchestrator-pods) our Kubernetes cluster.

### No specific semantics for DAGs

This is both a limitation and a feature. `mops` encourages you to
provide your own Python-based
[control flow between concurrent tasks](./orchestrator.md#concurrency),
meaning that you can write code like code, rather than code like
config.

However, if you're in a situation where you absolutely can't live
without some kind of DAG runner, `mops` won't provide that for
you. Happily, it _will_ stay out of your way and allow you to wrap any
sufficiently capable DAG runner around it, while allowing you to reap
the benefits of [memoization](./memoization.md) and efficient transfer
of control to the remote runtime.
