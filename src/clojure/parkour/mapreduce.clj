(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour (conf :as conf) (wrapper :as w) (cser :as cser)
             ,       (reducers :as pr)]
            [parkour.mapreduce (source :as src) (sink :as snk)]
            [parkour.util :refer [returning ignore-errors]])
  (:import [java.io Writer]
           [clojure.lang IFn$OOLL Var]
           [parkour.hadoop
            , EdnInputSplit KeyRecordSeqableRecordReader
            , KeyValueRecordSeqableRecordReader]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce TaskAttemptContext TaskAttemptID]
           [org.apache.hadoop.mapreduce TaskInputOutputContext]
           [org.apache.hadoop.mapreduce Counters CounterGroup Counter]))

(def ^:dynamic ^TaskInputOutputContext
  *context*
  "The task context.  Only bound during the dynamic scope of a task."
  nil)

(def ^:internal task-ex
  "Atom holding any exception thrown during local execution.  Intended only for
internal use within Parkour."
  (atom nil))

(defn local-runner?
  "True iff `conf` specifies the local job runner."
  [conf]
  (= "local" (or (conf/get conf "mapred.job.tracker" nil)
                 (conf/get conf "mapreduce.framework.name" nil)
                 "local")))

(defn keys
  "Produce keys only from the tuples in `context`.  Deprecated."
  [context] (src/shape-keys context))

(defn vals
  "Produce values only from the tuples in `context`.  Deprecated."
  [context] (src/shape-vals context))

(defn keyvals
  "Produce pairs of keys and values from the tuples in `context`.  Deprecated."
  [context] (src/shape-keyvals context))

(defn keygroups
  "Produce distinct keys from the tuples in `context`.  Deprecated."
  [context] (src/shape-keygroups context))

(defn valgroups
  "Produce sequences of values associated with distinct grouping keys from the
tuples in `context`.  Deprecated."
  [context] (src/shape-valgroups context))

(defn keyvalgroups
  "Produce pairs of distinct group keys and associated sequences of values from
the tuples in `context`.  Deprecated."
  [context] (src/shape-keyvalgroups context))

(defn keykeyvalgroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys and values from the tuples in `context`.  Deprecated."
  [context] (src/shape-keykeyvalgroups context))

(defn keykeygroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys from the tuples in `context`.  Deprecated."
  [context] (src/shape-keykeygroups context))

(defn keysgroups
  "Produce sequences of specific keys associated with distinct grouping keys
from the tuples in `context`.  Deprecated."
  [context] (src/shape-keysgroups context))

(defn source-as
  "Shape `source` to the collection shape `kind`.  The `kind` may either be a
source-shaping function of one argument or a keyword indicating a built-in
source-shaping function.  Supported keywords are: `:keys`, `:vals`, `:keyvals`,
`:keygroups`, `:valgroups`, :keyvalgroups`, `:keykeyvalgroups`,
`:keykeygroups`, and `:keysgroups`."
  [kind source] (src/source-as kind source))

(defn wrap-sink
  "Return new tuple sink which wraps keys and values as the types `ckey` and
`cval` respectively, which should be compatible with the key and value type of
`sink`.  Where they are not compatible, the type of the `sink` will be used
instead.  Returns a new tuple sink which wraps any sunk keys and values which
are not already of the correct type then sinks them to `sink`."
  ([sink] (snk/wrap-sink sink))
  ([ckey cval sink] (snk/wrap-sink ckey cval sink)))

(defn sink-as
  "Annotate `coll` as containing values to sink as `kind`.  The `kind` may
either be a sinking function of two arguments (a sink and a collection) or a
keyword indicating a built-in sinking function.  Supported keywords are `:none`,
`:keys`, `:vals`, and `:keyvals`."
  [kind coll] (snk/sink-as kind coll))

(defn sink
  "Emit all tuples from `coll` to `sink`, or to `*context*` if not provided."
  ([coll] (sink *context* coll))
  ([sink coll] ((snk/sink-fn coll) sink coll)))

(defn collfn
  "Task function adapter for collection-function-like functions.  The adapted
function `v` should accept conf-provided arguments followed by the (unwrapped)
input tuple source, and should return a reducible collection of output tuples.
If `v` has metadata for the `::mr/source-as` or `::mr/sink-as` keys, the
function input and/or output will be re-shaped as specified via the associated
metadata value."
  [v]
  (let [m (meta v)
        shape-in (get m ::source-as :default)
        shape-out (get m ::sink-as :default)]
    (fn [conf & args]
      (fn [context]
        (let [input (src/source-as shape-in (w/unwrap context))
              args (conj (vec args) input)
              output (snk/maybe-sink-as shape-out (apply v args))]
          (sink context output))))))

(defn contextfn
  "Task function adapter for functions accessing the job context.  The adapted
function `v` should accept a configuration followed by any conf-provided
arguments, and should return a function.  The returned function should accept
the job context and an (unwrapped) input tuple source, and should return a
reducible collection of output tuples."
  [v]
  (fn [conf & args]
    (let [f (apply v conf args)]
      (fn [context]
        (sink context (f context (w/unwrap context)))))))

(defn partfn
  "Partitioner function adapter for value-based partitioners.  The adapted
function `v` should accept a configuration followed by any conf-provided
arguments, and should return a function.  The returned function should accept
an (unwrapped) tuple key, (unwrapped) tuple value, and a partition-count; it
should return an integer modulo the partition-count, and should optionally be
primitive-hinted as OOLL."
  [v]
  (fn [& args]
    (let [f (apply v args)]
      (if (instance? IFn$OOLL f)
        (fn ^long [key val ^long nparts]
          (let [key (w/unwrap key), val (w/unwrap val)]
            (.invokePrim ^IFn$OOLL f key val nparts)))
        (fn ^long [key val ^long nparts]
          (let [key (w/unwrap key), val (w/unwrap val)]
            (f key val nparts)))))))

(defn ^:private ->rsrr
  "Record seqable record reader appropriate to keyword `kind`."
  [kind f]
  (case kind
    :keyvals (KeyValueRecordSeqableRecordReader. f)
    #_else (KeyRecordSeqableRecordReader. f)))

(defn recseqfn
  "Input format record-reader creation function adapter for input formats
implemented in terms of seqs.  The adapted function `v` should accept an input
split and a task context, and should return a value which is `seq`able,
`count`able, and optionally `Closeable`."
  [v]
  (let [kind (-> v meta (::source-as :keys))]
    (fn [split context & args]
      (->> (fn [split context]
             (let [split (if-not (instance? EdnInputSplit split)
                           split
                           @split)]
               (apply v split context args)))
           (->rsrr kind)))))

(def ^:private job-factory-method?
  "True iff the `Job` class has a static factory method."
  (->> Job reflect/type-reflect :members (some #(= 'getInstance (:name %)))))

(defmacro ^:private make-job
  "Macro to create a new `Job` instance, using Hadoop version appropriate
mechanism."
  [& args] `(~(if job-factory-method? `Job/getInstance `Job.) ~@args))

(defn job
  "Return new Hadoop `Job` instance, optionally initialized with configuration
`conf`."
  {:tag `Job}
  ([] (make-job (conf/ig)))
  ([conf] (make-job (conf/clone conf))))

;; The "mapred-{default,site}.xml" default resources are added to
;; `Configuration`s by the `JobConf` class static initializer.  Create (and
;; discard) one `Job` to ensure that static initializer -- or any future
;; resource-adding mechanism -- gets invoked.
(job)

(defmethod print-method Job
  [job ^Writer w]
  (.write w "#hadoop.mapreduce/job ")
  (print-method (conf/diff job) w))

(defn input-format!
  "Allocate and return a new input format class for `conf` as invoking `svar` to
generate input-splits and invoking `rvar` to generate record-readers.

During local job initialization, the function referenced by `svar` will be
invoked with the job context followed by any provided `sargs` (which must be
EDN-serializable); it should return a sequence of input split data.  Any values
in the returned sequence which are not `InputSplit`s will be wrapped in
`EdnInputSplit`s and must be EDN-serializable; in such a case, the `::mr/length`
and `::mr/locations` keys of such data may provide the split byte-size and node
locations respectively.

Prior to use, the function reference by `rvar` will be transformed by the
function specified as the value of the `readerv`'s `::mr/adapter` metadata,
defaulting to `parkour.mapreduce/recseqfn`.  During remote task-setup, the
transformed function will be invoked with the task input split and task context
followed by any provided `rargs`; it should return a `RecordReader` generating
the task input data.

See also: `recseqfn`."
  [conf svar sargs rvar rargs]
  (assert (instance? Var svar))
  (assert (instance? Var rvar))
  (let [i (conf/get-int conf "parkour.input-format.next" 0)
        k (format "parkour.input-format.%d" i)]
    (conf/assoc! conf "parkour.input-format.next" (inc i))
    (cser/assoc! conf
      (str k ".svar") svar, (str k ".sargs") sargs
      (str k ".rvar") rvar, (str k ".rargs") rargs)
    (Class/forName (format "parkour.hadoop.InputFormats$_%d" i))))

(defn mapper!
  "Allocate and return a new mapper class for `conf` as invoking `var`.

Prior to use, the function referenced by `var` will be transformed by the
function specified as the value of `var`'s `::mr/adapter` metadata, defaulting
to `parkour.mapreduce/collfn`.  During task-setup, the transformed function will
be invoked with the job `Configuration` and any provided `args` (which must be
EDN-serializable); it should return a function of one argument, which will be
invoked with the task context to execute the task.

See also: `collfn`, `contextfn`."
  [conf var & args]
  (assert (instance? Var var))
  (let [i (conf/get-int conf "parkour.mapper.next" 0)]
    (conf/assoc! conf "parkour.mapper.next" (inc i))
    (cser/assoc! conf
      (format "parkour.mapper.%d.var" i) var
      (format "parkour.mapper.%d.args" i) args)
    (Class/forName (format "parkour.hadoop.Mappers$_%d" i))))

(defn ^:private reducer!*
  [step conf var & args]
  (assert (instance? Var var))
  (let [i (conf/get-int conf "parkour.reducer.next" 0)]
    (conf/assoc! conf
      "parkour.reducer.next" (inc i)
      (format "parkour.reducer.%d.step" i) (name step))
    (cser/assoc! conf
      (format "parkour.reducer.%d.var" i) var
      (format "parkour.reducer.%d.args" i) args)
    (Class/forName (format "parkour.hadoop.Reducers$_%d" i))))

(defn reducer!
  "Allocate and return a new reducer class for `conf` as invoking `var`.

Prior to use, the function referenced by `var` will be transformed by the
function specified as the value of `var`'s `::mr/adapter` metadata, defaulting
to `parkour.mapreduce/collfn`.  During task-setup, the transformed function will
be invoked with the job `Configuration` and any provided `args` (which must be
EDN-serializable); it should return a function of one argument, which will be
invoked with the task context to execute the task.

See also: `collfn`, `contextfn`."
  [conf var & args]
  (apply reducer!* :reduce conf var args))

(defn combiner!
  "As per `reducer!`, but allocate and configure for the Hadoop combine step,
which may impact e.g. output types."
  [conf var & args]
  (apply reducer!* :combine conf var args))

(defn partitioner!
  "Allocate and return a new partitioner class for `conf` as invoking `var`.

Prior to use, the function referenced by `var` will be transformed by the
function specified as the value of `var`'s `::mr/adapter` metadata, defaulting
to the `(comp parkour.mapreduce/partfn constantly)`.  During task-setup, the
transformed function will be invoked with the job `Configuration` and any
provided `args` (which must be EDN-serializable); it should return a function of
three arguments: a raw map-output key, a raw map-output value, and an integral
reduce-task count.  That function will be called for each map-output tuple, must
return an integral value mod the reduce-task count, and must be primitive-hinted
as `OOLL`.

See also: `partfn`."
  [conf var & args]
  (assert (instance? Var var))
  (cser/assoc! conf
    "parkour.partitioner.var" var
    "parkour.partitioner.args" args)
  parkour.hadoop.Partitioner)

(defn set-mapper
  "Set the mapper class for `job` to `cls`."
  [^Job job cls] (.setMapperClass job cls))

(defn set-combiner
  "Set the combiner class for `job` to `cls`."
  [^Job job cls] (.setCombinerClass job cls))

(defn set-reducer
  "Set the reducer class for `job` to `cls`."
  [^Job job cls] (.setReducerClass job cls))

(defn set-partitioner
  "Set the partitioner class for `job` to `cls`."
  [^Job job cls] (.setPartitionerClass job cls))

(let [cfn (fn [c] (Class/forName (str "org.apache.hadoop.mapreduce." c)))
      klass (or (ignore-errors (cfn "task.TaskAttemptContextImpl"))
                (ignore-errors (cfn "TaskAttemptContext")))
      cname (-> ^Class klass .getName symbol)]
  (defmacro ^:private tac*
    [& args] `(new ~cname ~@args)))

(defprotocol ^:private GetTaskAttemptID
  "Protocol for extracting a task attempt ID."
  (^:private -taid [x] "Task attempt ID for `x`."))

(extend-protocol GetTaskAttemptID
  TaskAttemptContext (-taid [tac] (.getTaskAttemptID tac))
  TaskAttemptID (-taid [taid] taid))

(defn ^:private taid
  "Return `TaskAttemptID` for `x` when provided, or a new `TaskAttemptID`."
  ([] (TaskAttemptID. "parkour" 0 false 0 0))
  ([x] (-taid x)))

(defn tac
  "Return a new TaskAttemptContext instance using provided configuration `conf`
and task attempt ID `taid`."
  {:tag `TaskAttemptContext}
  ([conf] (tac conf (taid)))
  ([conf id] (tac* (conf/ig conf) (taid id))))

(defn counters-map
  "Translate job `counters` into nested Clojure map of strings to counts."
  [counters]
  (if (and counters (not (instance? Counters counters)))
    (->> counters meta ::counters counters-map)
    (let [c-entry (juxt #(.getName ^Counter %) #(.getValue ^Counter %))
          group (fn [g] (->> g (r/map c-entry) (into {})))
          g-entry (juxt #(.getName ^CounterGroup %) group)]
      (->> counters (r/map g-entry) (into {})))))
