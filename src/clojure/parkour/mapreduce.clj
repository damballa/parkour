(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour (conf :as conf) (wrapper :as w) (reducers :as pr)]
            [parkour.util :refer [returning]])
  (:import [java.util Comparator]
           [clojure.lang IPersistentCollection Indexed Seqable]
           [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.mapreduce
             Job MapContext ReduceContext TaskInputOutputContext]))

(defprotocol ^:private TupleSource
  "Internal protocol for iterating over key/value tuples from a source
of such tuples."
  (^:private ts-key [this]
    "Current tuple's key.")
  (^:private ts-val [this]
    "Current tuple's value.")
  (^:private ts-vals [this]
    "Current key's sequence of associated values.")
  (^:private ts-next-keyval [this]
    "Source updated to next key/value tuple.")
  (^:private ts-next-key [this]
    "Source updated to next distinct key."))

(defn ^:private ts-keyval
  "Pair of current tuple's key and value."
  [context] [(ts-key context) (ts-val context)])

(defn ^:private ts-keyvals
  "Pair of current tuple's key and sequence of associated values."
  [context] [(ts-key context) (ts-vals context)])

(defn ^:private ts-reduce
  "As per `reduce`, but in terms of `TupleSource` protocol.  When
provided, applies `nextf` to `context` to retrieve the next tuple
source for each iteration and `dataf` to retrieve the tuple values
passed to `f`."
  ([context f init]
     (ts-reduce ts-next-keyval ts-keyval f init))
  ([nextf dataf context f init]
     (loop [context context, state init]
       (if-let [context (nextf context)]
         (recur context (f state (dataf context)))
         state))))

(defn ^:private task-reducer
  "Make a tuple source `context` `reduce`able with particular
 iteration function `nextf` and extraction function `dataf`."
  [nextf dataf context]
  (reify ccp/CollReduce
    (coll-reduce [this f]
      (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init]
      (ts-reduce nextf dataf context f init))))

(extend-protocol TupleSource
  MapContext
  (ts-key [this] (.getCurrentKey this))
  (ts-val [this] (.getCurrentValue this))
  (ts-next-keyval [this] (when (.nextKeyValue this) this))

  ReduceContext
  (ts-key [this] (.getCurrentKey this))
  (ts-val [this] (.getCurrentValue this))
  (ts-vals [this] (.getValues this))
  (ts-next-keyval [this] (when (.nextKeyValue this) this))
  (ts-next-key [this] (when (.nextKey this) this)))

(extend-protocol ccp/CollReduce
  TaskInputOutputContext
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init] (ts-reduce this f init))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj]
    (reify
      Configurable
      (getConf [_] (conf/ig wobj))

      TupleSource
      (ts-key [_] (w/unwrap (ts-key wobj)))
      (ts-val [_] (w/unwrap (ts-val wobj)))
      (ts-vals [_] (r/map w/unwrap (ts-vals wobj)))
      (ts-next-keyval [this] (when (ts-next-keyval wobj) this))
      (ts-next-key [this] (when (ts-next-key wobj) this))

      ccp/CollReduce
      (coll-reduce [this f] (ts-reduce this f (f)))
      (coll-reduce [this f init] (ts-reduce this f init)))))

(defn keys
  "Produce keys only from the tuples in `context`."
  [context] (task-reducer ts-next-keyval ts-key context))

(defn vals
  "Produce values only from the tuples in `context`."
  [context] (task-reducer ts-next-keyval ts-val context))

(defn keyvals
  "Produce pairs of keys and values from the tuples in `context`."
  [context] (task-reducer ts-next-keyval ts-keyval context))

(defn keygroups
  "Produce distinct keys from the tuples in `context`."
  [context] (task-reducer ts-next-key ts-key context))

(defn valgroups
  "Produce sequences of values associated with distinct keys from the
tuples in `context`."
  [context] (task-reducer ts-next-key ts-vals context))

(defn keyvalgroups
  "Produce pairs of distinct keys and associated sequences of values
from the tuples in `context`."
  [context] (task-reducer ts-next-key ts-keyvals context))

(defprotocol ^:private TupleSink
  "Internal protocol for emitting tuples to a sink."
  (^:private -emit-keyval [sink key val]
    "Emit the tuple pair of `key` and `value` to `sink`.")
  (^:private -key-class [sink]
    "Key class expected by `sink`.")
  (^:private -val-class [sink]
    "Value class expected by `sink`."))

(defn ^:private key-class [sink] (-key-class sink))
(defn ^:private val-class [sink] (-val-class sink))

(extend-protocol TupleSink
  MapContext
  (-key-class [sink] (.getMapOutputKeyClass sink))
  (-val-class [sink] (.getMapOutputValueClass sink))
  (-emit-keyval [sink key val] (returning sink (.write sink key val)))

  ReduceContext
  (-key-class [sink]
    (case (-> sink .getConfiguration (conf/get "parkour.step" "reduce"))
      "combine" (.getMapOutputKeyClass sink)
      "reduce" (.getOutputKeyClass sink)))
  (-val-class [sink]
    (case (-> sink .getConfiguration (conf/get "parkour.step" "reduce"))
      "combine" (.getMapOutputValueClass sink)
      "reduce" (.getOutputValueClass sink)))
  (-emit-keyval [sink key val]
    (returning sink (.write sink key val))))

(defn ^:private wrapper-class
  [c c'] (if (isa? c c') c c'))

(defn wrap-sink
  "Return new tuple sink which wraps keys and values as the types
`ckey` and `cval` respectively, which should be compatible with the
key and value type of `sink`.  Where they are not compatible, the type
of the `sink` will be used instead.  Returns a new tuple sink which
wraps any sunk keys and values which are not already of the correct
type then sinks them to `sink`."
  [ckey cval sink]
  (let [conf (conf/ig sink)
        ckey (wrapper-class ckey (key-class sink)),
        wkey (w/new-instance conf ckey)
        cval (wrapper-class cval (val-class sink)),
        wval (w/new-instance conf cval)]
    (reify
      Configurable
      (getConf [_] conf)

      TupleSink
      (-key-class [_] ckey)
      (-val-class [_] cval)
      (-emit-keyval [sink1 key val]
        (returning sink1
          (let [key (if (instance? ckey key) key (w/rewrap wkey key))
                val (if (instance? cval val) val (w/rewrap wval val))]
            (-emit-keyval sink key val)))))))

(defn ^:private emit-keyval
  "Emit pair of `key` and `val` to `sink` as a complete tuple."
  [sink [key val]] (-emit-keyval sink key val))

(defn ^:private emit-key
  "Emit `key` to `sink` as the key of a key-only tuple."
  [sink key] (-emit-keyval sink key nil))

(defn ^:private emit-val
  "Emit `val` to `sink` as the value of a value-only tuple."
  [sink val] (-emit-keyval sink nil val))

(def ^:private emit-fn*
  "Map from sink-type keyword to tuple-emitting function."
  {:keyvals emit-keyval,
   :keys emit-key,
   :vals emit-val})

(defn sink-as
  "Return new tuple collection which has values sinked as `kind`,
which may be one of `:keys`, `:vals`, or `:keyvals`."
  [kind sink] (vary-meta sink assoc ::tuples-as kind))

(defn ^:private emit-fn
  "Tuple-emitting function for `coll`."
  [coll] (-> (meta coll) (get ::tuples-as :keyvals) emit-fn*))

(defn sink
  "Emit all tuples from `coll` to `sink`."
  [sink coll] (r/reduce (emit-fn coll) sink coll))

(def ^:private job-factory-method?
  "True iff the `Job` class has a static factory method."
  (->> Job reflect/type-reflect :members (some #(= 'getInstance (:name %)))))

(defmacro ^:private make-job
  "Macro to create a new `Job` instance, using Hadoop version
appropriate mechanism."
  [& args] `(~(if job-factory-method? `Job/getInstance `Job.) ~@args))

(defn job
  "Return new Hadoop `Job` instance, optionally initialized with
configuration `conf`."
  {:tag `Job}
  ([] (make-job (conf/ig)))
  ([conf] (make-job (conf/clone conf))))

(defn mapper!
  "Allocate and return a new parkour mapper class for `job` as
invoking `var`.  The `var` will be called during task-setup with the
job Configuration and any provided `args` (which must be
EDN-serializable).  It should return a function of one argument, which
will be invoked with the task context, and should perform the desired
content of the map task."
  [^Job job var & args]
  (let [i (conf/get-int job "parkour.mapper.next" 0)]
    (conf/assoc! job
      "parkour.mapper.next" (inc i)
      (format "parkour.mapper.%d.var" i) (pr-str var)
      (format "parkour.mapper.%d.args" i) (pr-str args))
    (Class/forName (format "parkour.hadoop.Mappers$_%d" i))))

(defn ^:private reducer!*
  [step ^Job job var & args]
  (let [i (conf/get-int job "parkour.reducer.next" 0)]
    (conf/assoc! job
      "parkour.reducer.next" (inc i)
      (format "parkour.reducer.%d.step" i) (name step)
      (format "parkour.reducer.%d.var" i) (pr-str var)
      (format "parkour.reducer.%d.args" i) (pr-str args))
    (Class/forName (format "parkour.hadoop.Reducers$_%d" i))))

(defn reducer!
  "Allocate and return a new parkour reducer class for `job` as
invoking `var`.  The `var` will be called during task-setup with the
job Configuration and any provided `args` (which must be
EDN-serializable).  It should return a function of one argument, which
will be invoked with the task context, and should perform the desired
content of the reduce task."
  [^Job job var & args]
  (apply reducer!* :reduce job var args))

(defn combiner!
  "As per `reducer!`, but allocate and configure for the Hadoop
combine step, which may impact e.g. output types."
  [^Job job var & args]
  (apply reducer!* :combine job var args))

(defn partitioner!
  "Allocate and return a new parkour partitioner class for `job` as
invoking `var`.  The `var` will be called during task-setup with the
job Configuration and any provided `args` (which must be
EDN-serializable).  It should return a function of three arguments: a
map-output key, a map-output value, and an integral reduce-task count.
That function will called for each map-output tuple, and should return
an integral value mod the reduce-task count.  Should be
primitive-hinted as OOLL."
  [^Job job var & args]
  (conf/assoc! job
    "parkour.partitioner.var" (pr-str var)
    "parkour.partitioner.args" (pr-str args))
  parkour.hadoop.Partitioner)
