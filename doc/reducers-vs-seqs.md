# Reducers versus lazy sequences

Most Parkour collection interfaces and operations default to working in terms of
Clojure reducers, but as of version 0.4.1 most support access via lazy sequences
as well.  It is generally not possible to mix reducer and lazy sequence
operations – although reducers may act on lazy sequences, reducers do not
support subsequent lazy realization of individual elements (at least not without
making trade-offs unacceptable for most applications).  As most functions and
function compositions much choose either reducers or lazy sequences, this
document describes some of the trade-offs involved in that choice.

Parkour’s default idiom uses reducers, which provides the following benefits
over lazy sequences:

- Improved performance.  By directly combining functions via function
  transformations, reducers avoid unnecessary allocations for intermediate
  operations, and can improve JVM JIT performance.
- Eager evaluation.  Many of the underlying Hadoop interfaces Parkour exposes as
  input collections internally use mutation to iterate through available tuples.
  Although reducers accumulate computations lazily, they evaluate eagerly once
  `reduce`d, fully processing each collection input value before moving on to
  the next.  This simplifies reasoning about the underlying Hadoop mutability,
  especially when working directly with Hadoop mutable serialization wrapper
  types.
- Integrated resource acquisition & release.  Reducers place all `reduce`
  computation within the dynamic scope of a function controlled by the reduced
  collection.  This allows the collection to control acquisition and release of
  necessary resources, such as dseqs’ Hadoop record readers and backing input
  streams.  In contrast, local dseq access via lazy seqs requires using the dseq
  `source-for` function via `with-open` etc.

Reducers do however also have some drawbacks lazy sequences do not:

- Smaller standard/extended library.  Reducers are still relatively new, and the
  standard library contains a smaller number of functions for working with them.
  Many operations are perfectly possible via reducers, but simply are not yet
  implemented, including e.g. `map-indexed`, `reductions`, etc.  Additionally,
  many useful third-party collection functions work in terms of seqs while
  having no existing reducer counterpart.
- Inexpressible operations.  Without JVM-level support for continuations or
  coroutines, some operations possible with lazy sequences cannot be expressed
  in terms of reducers.  In particular, any functions which require lazy
  realization of individual elements cannot be implemented for reducers without
  making generally unacceptable trade-offs such as cross-thread shipment of each
  collection value.  Most functions which need to perform multiple partial
  reduces over user-level partitionings of the input tuples will need to use
  lazy sequences.
- Single-pass only.  Although generally not advisable anyway, Parkour’s `reduce`
  for MapReduce task input does provide no means for reducing multiple times
  over the collection.  As per the backing Hadoop implementation, reduction
  mutates the input tuple stream, consuming tuples as they are processed.
  Subsequent reduction of a fully-reduced input will act as if reducing an empty
  collection.

I believe the weight of the trade-offs supports using reducers whenever
possible, but lazy sequences are available when not.
