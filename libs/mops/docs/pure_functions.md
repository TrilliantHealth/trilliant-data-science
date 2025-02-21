# `mops` and Pure functions

The overarching approach of `mops` is to realize the potential for _parallelizing_
[**_functions_**](https://en.wikipedia.org/wiki/Pure_function) using simple and scalable architectural
primitives. A function which depends on nothing but its input parameter(s) and produces no meaningful
result other than a returned value may be easily transposed into a different runtime context, and is
therefore easily parallelized. This is the principle behind `multiprocess.Pool`, behind the `MapReduce`
paradigm, and many other implementations of the same idea.

What this means for the user of this system is that you're going to have an easy time if your computation
is already encapsulated in a pure function, and a progressively harder time (more work to be done) the
less [pure](https://en.wikipedia.org/wiki/Pure_function) your existing computation is. You will almost
certainly need to refactor out any existing instances of impurity.

Practically speaking, some things to avoid/remove from your functions:

- Global mutable variables.
- Global constants (prefer passing the value of the constant as a function argument).
- Direct use of environment variables (again, pass values instead).
- Code that modifies input arguments, e.g. a dict or list.
- Any kind of randomness, including `datetime.now()` and friends.
- References to files or other state external to the function (with exceptions for certain narrow usages
  of filesystem primitives as described [here](./optimizations.md#paths))
- References to large amounts of static reference data. If possible, select only the data you need before
  passing it to the function. If not, see the docs on
  [large shared objects](./optimizations.md#large-shared-objects)
- Returning a result that does not contain _everything you might possibly want to know_ about the
  completed computation.

By implementing truly pure functions, we can keep the **What** (the business logic of your function)
separate from the [**How**](./basic_usage.md) (the details of what environment it runs on), which not
only enables plug-and-play parallelization but also makes your code much easier to read and reason about.

Ultimately, if your functions are _truly_ pure, you won't even need `mops` in the long run - you'll be
able to find other off-the-shelf libraries and frameworks that will let you parallelize your computation.
`mops` itself can be an implementation detail that doesn't fundamentally alter the code that you write,
which is in itself a valuable 'feature'.

## Logical (as opposed to theoretical) purity

A function that is theoretically/truly pure is one that performs absolutely no side effects. In a
distributed system, this may be difficult to achieve for various reasons.

A function that is _logically_ pure may perform side effects such as logging, uploading or downloading
data, writing to temporary files, performing arbitrary network communication, etc., _as long as_ the
results of those side effects are reasonably believed to be deterministic from the point of view of the
caller of the function.

In other words, if the calling your 'logically pure' function will result in the same return value each
time, and the side effects used to produce that result will not interfere with the operation of other
functions, then you may consider your function to be pure and it should work fine with `mops`.
