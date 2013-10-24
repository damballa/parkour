# Motivation for Parkour

Hadoop MapReduce provides a powerful platform for writing certain classes of
parallel programs.  Hadoop’s native Java interfaces for MapReduce applications
are highly flexible, but are also complex, weakly abstracted, and verbose.  Many
pre-existing higher-level frameworks built on top of Hadoop MapReduce attempt to
remedy the latter flaws, but invariably at the cost of some of the former
flexibility.

Parkour attempts to support the full capabilities of raw Hadoop MapReduce, but
with a concise and idiomatic Clojure interface.

## Goals

As a Clojure integration layer written directly against Hadoop, Parkour aims to
express the following properties:

- *Functional*.  Many abstractions in Hadoop are ultimately just functions in
  object clothing.  In Parkour, `Mapper`s, `Reducer`s, and `Partitioner`s are
  all replaced with plain Clojure functions bound to plain Clojure vars.
- *Dynamic*.  Hadoop and most Hadoop-based frameworks reconstitute remote task
  code by performing reflection or Java deserialization on instances of concrete
  types.  This approach is antithetical to Clojure, which tends towards run-time
  generation of non-seralizable closures.  In Parkour, task steps are
  reconstituted by requiring namespaces and invoking vars, which eliminates the
  need for concrete types or even ahead-of-time compilation.
- *Layered*.  Most Hadoop-based frameworks introduce their own computation
  abstractions and data-flow models.  These abstractions and models rarely map
  perfectly to the underlying platform, inopportunely leaking or concealing
  properties of Hadoop proper.  In Parkour, all new abstractions are
  intentionally leaky, providing explicit _layers_ of concise expression, while
  still exposing everything possible in raw MapReduce.
- *Idiomatic*.  Any Clojure code-base will already have many calls to the
  functions named `map` and `reduce`.  In Parkour, these functions sit at the
  heart of MapReduce tasks, acting in parallel on distributed sequences exactly
  as they would on local ones.

## Comparison to other frameworks

Parkour is not the first library to attempt to simplify Hadoop MapReduce, nor
the first library for Clojure integration with Hadoop.  Parkour attempts to
provide capabilities other libraries do not, and takes a different approach from
most pre-existing libraries and frameworks.  Comparison with a few of these
follows.

### Cascading and FlumeJava-likes (Scoobi, Crunch/Scrunch/Crackle, etc)

Several open source frameworks follow the model set by Google’s
[FlumeJava][flumejava], including [Scoobi][scoobi], Apache [Crunch][crunch], and
Crunch wrappers for Scala ([Scrunch][scrunch]) and Clojure ([Crackle][crackle]).
The [Cascading][cascading] framework pre-dates FlumeJava, but uses a very
similar model.

These libraries all present the same basic interface: A handful of distributed
collection types expose a small set of manipulation methods which lazily
accumulate a graph of applied computations.  An execution planner then optimizes
and distributes computation of the graph in terms of concrete MapReduce tasks.

Although a credible attempt, Crackle demonstrates the difficulties in
translating this approach to idiomatic Clojure.  The most fundamental is that
each individual user-supplied function in the computation graph must be
independently distributable.  The mechanism used for Java and Scala – capturing
all functions as Java-serializable objects – doesn’t interact well with
Clojure’s dynamism.  Other approaches either limit expressiveness (forcing all
individual operations to be bound to vars), or add significant complexity (deep
code re-writing).

Parkour pushes most composition of computation back to the language layer, as
explicit composition of Clojure functions within MapReduce task functions.  Task
functions act on the portion of a distributed collection available within an
individual task.  The prevents Parkour from providing explicit cross-task
operations, but allows task functions to call any Clojure collection function,
not just the subset of methods provided by a distributed collection type.  Users
must manually divide computations into tasks, but those tasks may combine into
any data-flow Hadoop allows, not just those the execution planner knows how to
build.

### Cascalog

[Cascalog][cascalog] is the elephant in the room.  Why Parkour when Cascalog
exists?  And especially when Cascalog 2 is right around the corner?

Cascalog and Cascading are both excellent pieces of engineering, but introduce
significant complexity.  Fundamentally, Cascalog is not an integration layer for
Hadoop.  Cascalog is its own abstraction layer, which supports Cascading as a
backend; Cascading in turn is also its own abstraction layer, which in turn
supports Hadoop as a backend.  These layers combined allow Cascalog queries to
run on Hadoop, but at significant remove from the underlying Hadoop platform,
while still depending on all its specifics.

Cascalog supports far more straightforward expression of query-like programs
than raw MapReduce does.  By elevating joins and cross-task operations to the
level of implicit behavior, Cascalog can very concisely express programs
involving these effects.  And yet, joins are not native to Hadoop, and in fact
general-purpose joins are relatively computationally expensive operations to
execute in MapReduce.  Moreover, Cascalog can make it frustratingly difficult or
impossible to perform certain computationally _inexpensive_ operations, such as
closing over data read from the distribute cache at task start-up, or writing to
multiple different outputs in a single task.

For programs which naturally and necessarily involve many joins, or generally
follow a query-like structure, Cascalog may be the best tool for the job.  For
many other programs, Parkour over raw MapReduce allows more straightforward and
efficient implementation.

### clojure-hadoop

The [clojure-hadoop][clojure-hadoop] project is much closer in approach to
Parkour than Cascalog, but swings the pendulum of abstraction too far in the
opposite direction.  Clojure-hadoop exposes the exact default MapReduce
interface used from Java, and uses `gen-class` and ahead-of-time compilation to
generate concrete task-implementation types.  This allows writing Java-style
computation over mutable state with a Java-style compilation workflow, only in
Clojure, which is less than optimal.  Clojure-hadoop was also a very early
project on the Clojure timeline – it exposes many idiosyncrasies where the
Clojure idiom had not yet settled, and provides no integration for features
which had not yet been added.

Clojure-hadoop is a historically-interesting library, but I believe Parkour to
expose a superset of its capabilities in a more modern and idiomatic fashion.

[cascalog]: https://github.com/nathanmarz/cascalog/
[cascading]: http://www.cascading.org/
[clojure-hadoop]: https://github.com/alexott/clojure-hadoop/
[flumejava]: http://pages.cs.wisc.edu/~akella/CS838/F12/838-CloudPapers/FlumeJava.pdf
[scoobi]: https://github.com/NICTA/scoobi
[crunch]: http://crunch.apache.org/
[scrunch]: http://crunch.apache.org/scrunch.html
[crackle]: https://github.com/viacoban/crackle
