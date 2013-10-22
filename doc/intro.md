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
