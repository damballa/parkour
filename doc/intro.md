# Introduction to Parkour

## Installation

Parkour is a Clojure library, distributed as a Maven artifact on Clojars, and is
designed to be used with a recent version of Leiningen (2.x, ideally 2.3.3 or
later).  To use, add the appropriate version of `com.damballa/parkour` to your
`project.clj`’s top-level `:dependencies` vector, then add appropriate versions
of the Hadoop artifact(s) to the `:provided` profile’s `:dependencies`.  Parkour
uses Clojure reducers, so for Java 6 you’ll also need to add `jsr166y`.  For
example:

```clj
(defproject ...
  ...
  :dependencies [...
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [com.damballa/parkour "0.4.1"]
                 ...]

  :profiles {...
             :provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "1.2.1"]]}
             ...}
  ...)
```

## Concepts

As much as possible, Parkour attempts to build directly upon abstractions
already provided by Hadoop and by Clojure, introducing new abstractions only as
necessary to bridge the gap between what Hadoop and Clojure provide.  The
following are the key ideas Parkour introduces or re-purposes.

### MapReduce via `reducers`

The Clojure 1.5 `clojure.core.reducers` standard library namespace narrows the
idea of a “collection” to “something which may be `reduce`d.”  This abstraction
allows the sequences of key-value tuples processed in Hadoop MapReduce tasks to
be represented as collections.  MapReduce tasks become functions over
collections, and may bring to bear not only Clojure’s standard library of
collection-manipulation operations, but any other functions implemented in terms
of those operations.

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
their single argument and modify that `Job` to apply some configuration.

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
implement the core Clojure `CollReduce` protocol, allowing any Hadoop job input
source to also be treated as a local reducible collection.

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
(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)
                     (graph :as pg) (tool :as tool)]
            [parkour.io (text :as text) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn mapper
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1])))))

(defn reducer
  [conf]
  (fn [context input]
    (->> (mr/keyvalgroups input)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)])))))

(defn word-count
  [conf dseq dsink]
  (-> (pg/input dseq)
      (pg/map #'mapper)
      (pg/partition [Text LongWritable])
      (pg/combine #'reducer)
      (pg/reduce #'reducer)
      (pg/output dsink)
      (pg/execute conf "word-count")
      first))

(defn tool
  [conf & args]
  (let [[outpath & inpaths] args
        input (apply text/dseq inpaths)
        output (seqf/dsink [Text LongWritable] outpath)]
    (->> (word-count conf input output)
         w/unwrap (into {})
         prn)))

(defn -main
  [& args] (System/exit (tool/run tool args)))
```

Let’s walk through some important features of this example.

### Task vars

The remote task vars (the arguments to the `map`, `combine`, and `reduce` calls)
have complete control over execution of their associated tasks.  The task vars
are higher-order functions which accept the job configuration plus any explicit
serialized parameters as arguments and return a task function.

In the default Hadoop Java interface, Hadoop calls a user-supplied method for
each input tuple.  Parkour instead calls the task function with the task context
and the entire set of local input tuples as a single reducible collection, and
expects a reducible output collection as the result.

The input collections are directly reducible as vectors of key/value pairs, but
the `parkour.mapreduce` namespace contains functions to efficiently reshape the
task-inputs, including `vals` to access just the input values and (reduce-side)
`keyvalgroups` to access grouping keys and grouped sub-collections of values.
Parkour also defaults to emitting the result collection as key/value pairs, but
`pakour.mapreduce` contains a `sink-as` function for specifying alternative
shapes for task output.

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

The return value of the `execute` function is a vector of dseqs for the graph
leaf node results.  These dseqs may be consumed locally as in the example, or
used as inputs for additional jobs.  When locally `reduce`d, dseqs yield
key-value vectors of the raw Hadoop wrapper objects produced by the backing
Hadoop input format.  The wrapper objects – or the dseqs themselves – may be
`unwrap`ed to directly access the contained values.
