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
                 [com.damballa/parkour "0.2.1"]
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

### Configuration steps

Hadoop and Java Hadoop libraries typically contain a number of static methods
for configuring Hadoop `Job` objects, handling such tasks as setting input &
output paths, serialization formats, etc.  Parkour codifies these as
_configuration steps_: functions which accept a `Job` object as their single
argument and modify that `Job` to apply some configuration.

In practice, Parkour configuration steps are implemented via a protocol
(`parkour.graph.cstep/ConfigStep`) and associated public application function
(`parkour.graph.cstep/apply!`), which allows for a handful of specializations:

- Functions – Clojure functions configure via direct application to a `Job`.
- Maps – Clojure maps are merged into a `Job` as key/value parameters.
- Vectors – Clojure vectors apply each member in sequence as separate
  configuration steps.
- dseqs & dsinks – Reified configuration steps, described in the following
  sub-sections.

#### Distributed sequences (dseqs)

A Parkour distributed sequence configures a job for input from a particular
location and input format, reifying a function calling the underlying Hadoop
`Job#setInputFormatClass` etc methods.  In addition to `ConfigStep`, dseqs also
implement the core Clojure `CollReduce` protocol, allowing any Hadoop job input
source to also be treated as a local reducible collection.

#### Distributed sinks (dsinks)

A Parkour distributed sink configures a job for output to a particular location
and output format, reifying a function calling the underlying Hadoop
`Job#setOutputFormatClass` etc methods.  In addition to `ConfigStep`, dsinks
also implement the Parkour `DSeqable` protocol, which yields a dseq for reading
back the results of writing to the dsink.

### Job graph

The `parkour.graph` API provides an interface for assembling collections of
configuration steps and remote task functions into _job nodes_.  It allows
chaining those nodes into a _job graph_ DAG of multiple Hadoop job nodes, where
later jobs consume the results of earlier jobs as input.  The job graph captures
the high-level data-flow through a set of jobs.  The job graph API allows you to
specify that data-flow in a straightforward way.

During construction, each job node has a _stage_.  Functions in the graph API
update a node’s stage as they layer additional configuration onto the node.
Most stages have associated functions in the graph API.

- `:source` – A job node configured for input, but with no actual job execution
  step.  The `source` function creates fresh `:source`-stage nodes from dseqs.
- `:map` – A job configured with a map task.  The `remote` function will add a
  map task function to a `:source` or `:map` node, yielding a new `:map` node.
- `:partition` – A job configured with map output and partitioning.  The
  `partition` function will add partitioning to `:map` node or co-grouping to a
  vector of `:map` nodes, yielding a new `:partition` node.
- `:reduce` – A job configured with a reduce task.  The `remote` function will
  add a reduce task function to a `:partition` or `:reduce` node, yielding a new
  `:reduce` node.
- `:sink` – A job configured through final output to a dsink.  The `sink`
  function will add sinking to a node at any other stage, yielding a new
  `:source` node which depends upon and consumes the results of the finished
  `:sink` node’s job.  The `sink-multi` function acts similarly, but configures
  multiple dsinks as named outputs, and yields a vector of new source nodes.

Additionally, the `config` function will add arbitrary configuration steps,
yielding a new node in the same stage, with those steps added.

### Remote invocation via Vars

Parkour runs all remote functions by `require`ing a Clojure namespace and
invoking a Clojure var, usually optionally with any number of EDN-serializable
arguments.  In the Parkour APIs, these vars are always explicitly specified as
such – these is no automatic implicit serialization.

In the job graph API, the explicit remote entry point is the function which
builds the job graph.  Although this does require the graph-builder be a pure
function, it also allows job task functions to be any Clojure function, without
needing to create special “serializable functions” or tie all individual
operations to vars.

## Example

Here’s the complete classic “word count” example written using Parkour:

```clj
(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (mapreduce :as mr) (graph :as pg)]
            [parkour.io (text :as text)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count
  [[] [dseq dsink]]
  (-> (pg/source dseq)
      (pg/remote
       (fn [input]
         (->> input mr/vals
              (r/mapcat #(str/split % #"\s+"))
              (r/map #(-> [% 1])))))
      (pg/partition [Text LongWritable])
      (pg/remote
       (fn [input]
         (->> input mr/keyvalgroups
              (r/map (fn [[word counts]]
                       [word (r/reduce + 0 counts)])))))
      (pg/sink dsink)))

(defn -main
  [& args]
  (let [[inpath outpath] args
        input (text/dseq inpath)
        output (text/dsink outpath)]
    (pg/execute (conf/ig) #'word-count [] [input output])))
```

Let’s walk through some important features of this example.

### Task functions

The remote task functions (the arguments to the `remote` calls) have complete
control over execution of their associated tasks.  In the default Hadoop Java
interface, Hadoop calls a user-supplied method for each input tuple.  Parkour by
default instead calls the user-supplied task function with the entire set of
input tuples as a single reducible collection, and expects a reducible
collection as the result.

The input collections are directly reducible as vectors of key/value pairs, but
the `parkour.mapreduce` namespace contains functions to efficiently reshape the
task-inputs, including `vals` to access just the input values and (reduce-side
only) `keyvalgroups` to access grouping keys and grouped sub-collections of
values.

Parkour also defaults to emitting the result collection as key/value pairs, but
`pakour.mapreduce` contains a `sink-as` function for specifying alternative
shapes for task output.

### Automatic wrapping & unwrapping

Hadoop jobs generally do not act directly on values, instead using instances of
wrapper types — such as `Writable`s – which implement their own serialization
and/or are tied to a serialization method registered with the Hadoop
serialization framework.  Parkour by default automatically handles unwrapping
any input objects and re-wrapping any output objects which are not compatible
with the configured task output type.  The example job use the `Writable` `Text`
and `LongWritable` types, but the task code deals entirely with native string
and integers.

### Remote and local-only arguments

Not everything is serializable, and it is frequently useful to use
non-serializable values (such as configuration steps) when constructing the job
graph.  The `parkour.graph` API supports this by accepting two separate argument
vectors in the `execute` function, and passing them as separate vectors to the
user-provided graph-building var.

Parkour EDN-serializes the first vector into the job configuration, and passes
those same arguments to each remote invocation of the graph-building function.
This gives task functions easy access to job parameters etc.  Parkour only
passes the second vector when it calls the graph-builder locally to assemble the
set of jobs to execute.  Remote tasks will receive `nils`.

This remote/local separation makes graph-building more flexible, but you must be
careful to ensure that local-only arguments do not affect the shape of the
constructed graph or the execution of task functions.  If the local and remote
job graphs will not match, task execution will not work correctly.

### Results

Although not shown in this example, the return value of the `execute` function
is a vector of dseqs for the graph tail node results.  These dseqs may be
consumed locally, or passed as inputs to additional job graphs.
