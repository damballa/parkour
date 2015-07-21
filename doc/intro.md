# Introduction to Parkour

## Installation

Parkour is a Clojure library, distributed as a Maven artifact on Clojars, and is
designed to be used with a recent version of Leiningen (2.x, ideally 2.3.3 or
later).  To use, add the appropriate version of `com.damballa/parkour` to your
`project.clj`’s top-level `:dependencies` vector, then add appropriate versions
of the Hadoop artifact(s) to the `:provided` profile’s `:dependencies`.  For
example:

```clj
(defproject ...
  ...
  :dependencies [...
                 [com.damballa/parkour "0.6.3"]
                 ...]

  :profiles {...
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-client "2.4.1"]
               [org.apache.hadoop/hadoop-common "2.4.1"]]}
             ...}
  ...)
```

## Concepts

As much as possible, Parkour attempts to build directly upon abstractions
already provided by Hadoop and by Clojure, introducing new abstractions only as
necessary to bridge the gap between what Hadoop and Clojure provide.  The
following are the key ideas Parkour introduces or re-purposes.

### MapReduce via `reducers` (and lazy seqs)

The Clojure >=1.5 `clojure.core.reducers` standard library namespace narrows the
idea of a “collection” to “something which may be `reduce`d.”  This abstraction
allows the sequences of key-value tuples processed in Hadoop MapReduce tasks to
be represented as collections.  MapReduce tasks become functions over
collections, and may bring to bear not only Clojure’s standard library of
collection-manipulation operations, but any other functions implemented in terms
of those operations.

When necessary, Parkour also allows tasks to operate on inputs as lazy
sequences.  See [reducers vs seqs][reducers-vs-seqs] for a discussion of the
trade-offs involved.

[reducers-vs-seqs]: https://github.com/damballa/parkour/blob/master/doc/reducers-vs-seqs.md

### Remote invocation via vars

Parkour runs all remote (job-distributed) functions by `require`ing a Clojure
namespace and invoking a Clojure var, usually optionally with any number of
EDN-serializable arguments.  In the Parkour APIs, these vars are always
explicitly specified as such – there is no automatic implicit serialization.

Clojure vars are more flexible than Java classes, and don’t require concrete
types or ahead-of-time compilation to find via reflection.  Vars are less
flexible than arbitrary functions, but don’t require any code re-writing or
brittle “magic” to find and invoke from remote tasks.

### Configuration steps (csteps)

Hadoop and Java Hadoop libraries typically contain a number of static methods
for configuring Hadoop `Job` objects, handling such tasks as setting input &
output paths, serialization formats, etc.  Parkour codifies these as
_configuration steps_, or _csteps_: functions which accept a `Job` object as
their single argument and modify that `Job` to apply some configuration.  The
crucial difference is that csteps are themselves first-class values, allowing
dynamic assembly of job configurations from the composition of opaque
configuration elements.

In practice, Parkour configuration steps are implemented via a protocol
(`parkour.cstep/ConfigStep`) and associated public application function
(`parkour.cstep/apply!`), which allows for a handful of specializations:

- Functions – Clojure functions configure via direct application to a `Job`.
- Maps – Clojure maps are merged into a `Job` as key/value parameters.
- Vectors – Clojure vectors apply each member in sequence as separate
  configuration steps.
- dseqs & dsinks – Reified csteps, described in the following sub-sections.

#### Distributed sequences (dseqs)

A Parkour distributed sequence configures a job for input from a particular
location and input format, reifying a function calling the underlying Hadoop
`Job#setInputFormatClass` etc methods.  In addition to `ConfigStep`, dseqs also
implement the core Clojure `CollReduce` and `CollFold` protocols, allowing any
Hadoop job input source to also be treated as a local reducible and foldable
collection.

#### Distributed sinks (dsinks)

A Parkour distributed sink configures a job for output to a particular location
and output format, reifying a function calling the underlying Hadoop
`Job#setOutputFormatClass` etc methods.  Dsinks are reified so that they can
also implement the Parkour `DSeqable` protocol, which yields a dseq for reading
back the results of writing to the dsink.  For testing etc, the `dsink/sink-for`
function opens a local key/value tuple sink from a dsink, allowing local
creation of any output a MapReduce job could produce.

### Job graph

The `parkour.graph` API provides an interface for assembling collections of
configuration steps into _job nodes_.  It allows chaining those nodes into a
_job graph_ DAG of multiple Hadoop job nodes, where later jobs consume the
output(s) of earlier jobs as input.  A job graph captures the high-level
data-flow through a set of jobs.  The job graph API allows specification of that
data-flow in a straightforward manner.

During construction, each job node has a _stage_.  Each stage has an associated
function in the graph API which advances a node to that stage while adding
appropriate configuration.  These functions ensure a basic level of internal
consistency for job configurations, while also providing a shorthand for common
configuration steps.

The core functions and associated stages are: `input`, `map`, `partition`,
`combine`, `reduce`, and `output`.  Additionally, the generic `config` function
allows adding arbitrary configuration steps to a job node in any stage.

## Example

Here’s the complete classic “word count” example, written using Parkour:

```clj
(ns parkour.example.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (toolbox :as ptb) (tool :as tool)]
            [parkour.io (text :as text) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count-m
  [coll]
  (->> coll
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))

(defn word-count
  [conf lines]
  (-> (pg/input lines)
      (pg/map #'word-count-m)
      (pg/partition [Text LongWritable])
      (pg/combine #'ptb/keyvalgroups-r #'+)
      (pg/output (seqf/dsink [Text LongWritable]))
      (pg/fexecute conf `word-count)))

(defn tool
  [conf & inpaths]
  (->> (apply text/dseq inpaths)
       (word-count conf)
       (into {})
       (prn)))

(defn -main
  [& args] (System/exit (tool/run tool args)))
```

Let’s walk through some important features of this example.

### Task vars & adapters

The remote task vars (the arguments to the `map` and `combine` calls) have
complete control over execution of their associated tasks.  The underlying
interface Parkour exposes models the Hadoop `Mapper` and `Reducer` classes as
higher-order function, with construction/configuration invoking the initial
function, then task-execution invoking the function returned by the former.

Most tasks don’t need this complexity, so Parkour allows vars to specify via
metadata an “adapter function” for transforming the var’s value to the
underlying interface.  The default adapter function allows idiomatic Clojure
collection functions to be used as tasks, but others allow direct access to the
job `Configuration` and `JobContext`.  For example, `identity` as an adapter
function allows total control over raw task execution via the fundamental
Parkour-Hadoop interface.

### Inputs as collections

In the default Hadoop Java interface, Hadoop calls a user-supplied method for
each input tuple, which is then expected to write zero or more output tuples.
Parkour instead calls task functions with the entire set of local input tuples
as a single reducible collection, and expects a reducible output collection as
the result.

In the underlying Hadoop interfaces, all input and output happens in terms of
key/value pairs.  Parkour’s input collections are directly reducible as vectors
of those key/value pairs, but also support efficient “reshaping” to iterate over
just the relevant data for a particular task.  These shapes include `vals` to
access just the input values and (reduce-side) `keyvalgroups` to access grouping
keys and grouped sub-collections of values.  The `collfn` adapter allows
specifying these shapes as keywords via `::mr/source-as` metadata on task vars.

Parkour usually defaults to emitting the result collection as key/value pairs,
but the `collfn` adapter supports `::mr/sink-as` metadata for specifying
alternative shapes for task output.

Additionally, dseqs and dsinks may supply different default input and output
shapes.  The text dseq used in the word count example specifies `vals` as the
default, allowing unadorned task vars to receive the input collection as a
collection of the input text lines (versus the underlying Hadoop tuples of file
offset and text line).

### Automatic wrapping & unwrapping

Hadoop jobs generally do not act directly on values, instead using instances of
wrapper types – such as `Writable`s – which implement their own serialization
and/or are tied to a serialization method registered with the Hadoop
serialization framework.  Parkour by default automatically handles unwrapping
any input objects and re-wrapping any output objects which are not compatible
with the configured task output type.  The example job uses the `Writable`
`Text` and `LongWritable` types, but the task code deals entirely with native
strings and integers.

This auto-un/wrapping is entirely optional, and programs may exchange the
convenience of native values for the performance advantages of working directly
with Hadoop’s serialization containers.

### Results

The return value of the `fexecute` function is a dseq for the job graph leaf
node result.  That dseq may be consumed locally as in the example, or used as
the input for additional jobs.  When locally `reduce`d, dseqs yield key-value
vectors of the `unwrap`ed values of the objects produced by the backing Hadoop
input format.  The `parkour.io.dseq/source-for` function can provide direct
access to the raw wrapper objects, as well as allowing dseqs to be realized as
lazy sequences instead of reducers.
