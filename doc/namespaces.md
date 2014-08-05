# Parkour namespaces guide

Parkour’s namespaces fall into a handful of different categories, each described
below.

## Basic Hadoop integration

Parkour’s basic Hadoop integration namespaces provide convenient Clojure access
to the core Hadoop APIs necessary for writing MapReduce jobs.

### parkour.conf

The `parkour.conf` namespace contains functions for manipulating Hadoop
`Configuration` objects as bash-in-place mutable maps.  It provides type-aware
string conversion on parameter-set, type-specific functions for parameter
access, and automatic conversion for the wrapped `Configuration`s of objects
such as `Job`s and `Configurable`s.  It also provides a dynamically-scoped base
default `Configuration`, and a tagged literal form for `Configuration`s as a map
of differences from the default.

```clj
(require '[parkour.conf :as conf])

(conf/ig)
;;=> #hadoop.conf/configuration {}
(conf/assoc! *1 "foo" 1, "baz" [1 2 3])
;;=> #hadoop.conf/configuration {"baz" "1,2,3", "foo" "1"}
```

### parkour.fs

The `parkour.fs` namespace contains functions for manipulating Hadoop `Path` and
`FileSystem` objects.  It provides Hadoop integration for the standard library
IO facilities, functions simplifying common filesystem operations, and tagged
literal forms for Hadoop `Path` and `java.net.URI` objects.

```clj
(require '[parkour.fs :as fs])

(-> "*.clj" fs/path-glob first)
;;=> #hadoop.fs/path "file:/home/llasram/ws/parkour/project.clj"
(-> *1 slurp (subs 0 32))
;;=> "(defproject com.damballa/parkour"
```

### parkour.tool

The `parkour.tool` namespace provides functions to wrap Clojure functions as
Hadoop `Tool`s and to run Clojure functions via the Hadoop `ToolRunner`.  This
provides a simple standard interface for implementing command-line tools which
allow end-users to set arbitrary Hadoop configuration parameters at run-time.

```clj
(require '[parkour.tool :as tool])

(defn tool
  [conf & args]
  ...)

(defn -main
  [& args]
  (System/exit (tool/run tool args)))
```

### parkour.wrapper

The `parkour.wrapper` namespace provides the `Wrapper` protocol, which allows
generic Clojure access to `Writable`s and other mutable serialization classes
fitting the default Hadoop idiom.

The protocol `unwrap` function extracts a native value from a wrapper object.
The `rewrap` function mutates a wrapper object to wrap a new value.  The
multimethod-backed `new-instance` function creates a fresh instance of an
arbitrary wrapper class, as per most `Writable`s’ zero-argument constructors.

The `parkour.wrapper` namespaces contains implementations for many of the most
common classes, with others easily provided by the user.  Much of the
namespace’s functionality has sensible default implementations, and the
`auto-wrapper` macro can automatically define a `Wrapper` implementation for
many common types.

```clj
(require '[parkour.wrapper :as w])
(import '[org.apache.hadoop.io VLongWritable])

(w/auto-wrapper VLongWritable)

(w/new-instance VLongWritable)
;;=> #<VLongWritable 0>
(w/rewrap *1 31337)
;;=> #<VLongWritable 31337>
(w/unwrap *1)
;;=> 31337
```

### parkour.mapreduce

The `parkour.mapreduce` namespace provides Clojure MapReduce task integration,
allowing MapReduce tasks to be implemented by Clojure functions bounds to vars.

For each supported Hadoop base class, Parkour internally maintains pools of
static classes which use their Hadoop `Configuration`s to specify a namespace
and var to load and invoke.  The `mapper!`, `reducer!`, etc binding functions
modify a `Configuration` (or `Job`) to allocate a class of the associated type
and bind it to a particular var.

These functions do _not_ directly configure a job to use the class.  Instead,
they return the configured class, allowing the user to then pass that class to
any function or Hadoop method expecting a class of that type.  These include the
standard `.setMapperClass` etc methods, but also such methods as
`MultipleInputs`’ `.addInputPath`.  This approach allows Parkour to work cleanly
with Hadoop interfaces which use multiple classes of the same abstract type, as
with `MultipleInputs`’ use of multiple `Mapper` classes.

The invoked vars follow a uniform higher-order function interface, where during
task-setup Parkour invokes the var-functions with the job `Configuration`
followed by any (EDN-serialized) class-var binding arguments.  The var-functions
then return functions (or other objects) which implement the functionality of
the bound class.  The interfaces for these functions are class-specific, and
documented in the docstrings of the associated binding functions.

Because this interface can be unwieldy, Parkour transforms most var-bound
objects via “adapter functions.”  Each var-type mapping has a type-specific
default adapter; alternative adapters may be provided by the user via the value
of the var’s `:parkour.mapreduce/adapter` metadata.

The namespace also provides functions for efficiently reshaping both task input
and output collections, and for writing output tuples to the task context.
Unlike the Java raw MapReduce interfaces, Parkour provides access to each
individual input tuple in both map and reduce tasks, as collections of these
tuples.  The input reshaping functions allow access to key/value tuples, just
keys or values, or – in reduce tasks – any combination of distinct grouping keys
and reshaped sub-collections of grouped tuples.

Generally the reshaping functions do not need to be invoked directly, and
instead can be specified as keyword values to the `::mr/source-as` and
`::mr/sink-as` metadata keys used by the `collfn` adapter.

```clj
(require '[clojure.core.reducers :as r])
(require '[parkour.mapreduce :as mr])

(defn replacer
  {::mr/source-as :keys, ::mr/sink-as :keyvals}
  [val coll]
  (r/map (fn [key] [key val]) coll))

(defn run-replacer
  [val]
  (let [job (mr/job)]
    (doto job
      ;; Set job name, JAR, input format, etc
      ...
      ;; Set mapper via Parkour
      (.setMapperClass (mr/mapper! job #'replacer val))
      (.setNumReduceTasks 0))
    (.waitForCompletion job)))
```

## Job configuration

Parkour provides a higher-level API for configuring Hadoop jobs in terms of
“configuration steps.”  Configuration steps are simply functions which invoke
Hadoop- or library-provided methods for job configuration, but which capture
step parameters as closed-over variables, exposing an entirely uniform interface
for applying configuration steps to jobs.  This allows code to treat
configuration steps as first-class entities, inverting the control pattern
exposed by the base Hadoop Java API.

### parkour.cser

The `parkour.cser` namespace provides functions for writing/reading Clojure
values to/from job configuration parameters as extended EDN:

- `assoc!` – Sets a configuration parameter to the EDN representation of a
  value.
- `get` – Reads a value from the EDN representation stored in a configuration
  parameter.

In addition to the obvious, these functions provide the following behaviors:

- For the dynamic scope of calls to `assoc!`, `parkour.cser/*conf*` will be
  bound to the configuration being modified.  This provides `print-method`
  implementations access to the configuration a value is being serialized into,
  allowing them to access and modify other parameters as necessary to support
  their serialization.
- The printed form of Clojure vars (e.g. `#'foo/bar`) is read as `require`ing
  the containing namespace then `resolve`ing the symbol.  This supports
  e.g. higher-order tasks parameterized with other function-vars.

### parkour.cstep

The `parkour.cstep` namespace provides the `ConfigStep` protocol.  Although
configuration steps are conceptually just functions over `Job` objects,
implementation in terms of a protocol yields certain practical advantages.  The
namespace provides the protocol-backed `apply!` function for applying an
arbitrary configuration step to a job.

The namespace also provides a few base implementations of the `ConfigStep`
protocol, which cover the majority of common uses:

- Functions – All single-argument functions are configuration steps, applied by
  invoking the function on a provided `Job` instance.
- Maps – Clojure maps are applied by setting the value of each `Job`-parameter
  map key to the associated map value.
- Vectors – Clojure vectors are applied by applying each vector member as a
  configuration step, allowing easy composition of configuration step.

```clj
(require '[parkour.config :as config])
(require '[parkour.mapreduce :as mr])
(require '[parkour.cstep :as cstep])

(->> [{"foo" "bar"}
      (fn [job] (conf/assoc! job "baz" "quux"))]
     (cstep/apply! (mr/job)))
;;=> #hadoop.mapreduce/job {"baz" "quux", "foo" "bar"}
```

### parkour.io.*

The collection of namespaces under `parkour.io` extends configuration steps with
specific integration for job input and output.

The `parkour.io.dseq` namespace provides the `dseq` function for reifying a
job-input configuration step as a “distributed sequence.”  As with any other
configuration step, the backing configuration step functions simply call
underlying job-configuration methods.

In addition to acting as the configuration steps they wrap, distributed
sequences also act as locally-reducible collections.  This provides seamless
local access to the input or output of any Hadoop job.  Distributed sequences
may be directly reduced as unwrapped tuples, or opened with the
`dseq/source-for` function.  The `source-for` function takes an optional `:raw?`
keyword-argument which allows direct access to the raw Hadoop wrapper type
instances, and its return value is `seq`able as well as `reduce`-able.

The `parkour.io.dsink` namespace provides the `dsink` function for reifying a
job-output configuration step as a “distributed sink.”  As with dseqs, dsinks
simply wrap functions calling existing Hadoop methods.  The `dsink` function
however takes a second parameter, which is a dseq for consuming as input the
results of a job writing to the dsink.  This allows abstract specification of
job-chains, where subsequent jobs consume as input the output of previous jobs.
When called on a dsink, the `dseq` function will return this mirroring dseq.

The `dsink/sink-for` function allows any `dsink` to be opened for writing
locally.  The same `mr/sink` function used in job task functions will write
local collections to these local sinks, producing the same output as when
running a job.  This simplifies the creation of test fixtures, etc.

The `parkour.io.dval` namespace provides a value-oriented interface for
interacting with the Hadoop distributed cache.  It is described in detail in the
documentation for [distributed values][dvals].

Most of the remaining namespaces provide pre-built dseq and dsink
implementations for common input and output formats:

- `parkour.io.text` – Line-oriented text files.
- `parkour.io.nline` – Line-oriented text files, with `n` lines per map task.
- `parkour.io.seqf` – Hadoop sequence files.
- `parkour.io.cascading` – Cascading sequence files.
- `parkour.io.mem` – Memory-based input, for testing.
- `parkour.io.avro` – Clojure-customized Avro input and output via
  [Abracad][abracad].  In addition to `dseq` and `dsink` functions for creating
  Avro dseqs and dsinks, also provides a `shuffle` function for configuring an
  Avro-serialized shuffle.

Two namespaces provide general facilities, distinct from particular concrete
input or output formats:

- `parkour.io.mux` – General multiple inputs; and
- `parkour.io.dux` – General multiple outputs.

The multiple inputs/outputs provided by these namespaces differ from the
standard Hadoop `MultipleInputs` and `MultipleOutputs` classes by proxying the
full `InputFormat` and `OutputFormat` interfaces across sub-format instances
configured with complete distinct sub-configurations.  This allows different
inputs/outputs to use different values for any parameter isolated to the
input/output format.  This isolation removes the need for special-purpose
de/muxing classes like `AvroMultipleOutputs`.

```clj
(require '[clojure.core.reducers :as r])
(require '[parkour.io.text :as text])

(->> (text/dseq "project.clj")
     (r/map #(subs % 0 32))
     (r/take 1)
     (into []))
;;=> ["(defproject com.damballa/parkour"]

;; Or with multiple inputs
(->> (apply text/dseq (fs/path-glob "*.clj"))
     (r/map #(subs % 0 32))
     (r/take 1)
     (into [])))
;;=> ["(defproject com.damballa/parkour"]
```

## Job graph API

The `parkour.graph` namespace provides the Parkour job graph API.  The job graph
API is a functional internal DSL for assembling configuration steps into _job
nodes_ containing complete job configurations, and for assembling job nodes into
_job graphs_ of multi-job processing pipelines.

A job node consists primarily of a list of configuration steps, a list of
dependency job nodes, and a _job stage_.  The job stage captures the state of a
job node in the course of a relatively linear process of adding all the
configuration steps necessary to produce a complete job.  Each stage has an
associated API function which produces a node in that stage while adding an
associated configuration step.

The job graph API functions and associated stages are as follows:

- `input` – Accepts an input dseq; returns a new `:input`-stage node with the
  dseq as its initial step.  This is the sole graph API function which does not
  act on an existing job node.
- `map` – Accepts a `:input` node and a map-task var or class; returns a `:map`
  node with a configuration step specifying that mapper.  Instead of a single
  `:input` node, also accepts a vector of `:input` nodes, which are configured
  as multiplex input to the mapper.
- `partition` – Accepts a `:map` node, a shuffle configuration step, and an
  optional partitioner var or class; returns a `:partition` node.  Instead of a
  single `:map` node, also accepts a vector of `:map` nodes, which are
  configured as multiplex input to the partitioner.  Instead of a configuration
  step, also accepts a vector of two classes, which are configured as the map
  output key and value classes for a basic shuffle.
- `combine` – Accepts a `:partition` node and a combine-task var or class;
  returns a `:combine` node.  The provided combiner will double as the job
  reducer if a separate reducer is not later specified.
- `reduce` – Accepts a `:partition` or `:combine` node and a reduce-task var or
  class; returns a `:reduce` node.
- `output` – Accepts a `:map` or `:combine`/`:reduce` node and an output dsink;
  returns a `:input` node which consumes from the provided dsink’s associated
  dseq and depends on the job node completed by the dsink’s configuration step.
  Instead of a single output dsink, also accepts multiple arguments as a
  sequence of name-dsink outputs, which are configured as demultiplex named
  outputs; returns a vector of `:input` nodes for the associated dseqs.

In addition to the per-stage functions, the job graph API also provide a generic
`config` function, which adds arbitrary configuration steps to a node in any
stage.

The job graph API `execute` function will execute the jobs produced by a graph
of job nodes.  The `execute` function accepts either a single job graph leaf
node or a vector of such nodes, a base `Configuration`, and a job basename.  It
runs the jobs composing the graph, attempting to run independent jobs in
parallel.  On successful completion, it returns a vector of the provided leaf
node dseqs, and on failure throws an exception.

For the common case where a job graph yield only a single output, the `fexecute`
function will verify that there is only a single output and return it.

```clj
(defn word-count
  [conf lines]
  (-> (pg/input lines)
      (pg/map #'word-count-m)
      (pg/partition [Text LongWritable])
      (pg/combine #'ptb/keyvalgroups-r #'+)
      (pg/output (seqf/dsink [Text LongWritable]))
      (pg/fexecute conf `word-count)))
```

[abracad]: https://github.com/damballa/abracad/
[dvals]: https://github.com/damballa/parkour/blob/master/doc/dvals.md
