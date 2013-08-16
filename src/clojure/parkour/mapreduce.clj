(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour.wrapper :as w]
            [parkour.util :refer [returning]]
            [parkour.reducers :as pr])
  (:import [java.util Comparator]
           [clojure.lang IPersistentCollection Indexed Seqable]
           [org.apache.hadoop.conf Configuration]
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
    "Emit the tuple pair of `key` and `value` to `sink`."))

(extend-protocol TupleSink
  TaskInputOutputContext
  (-emit-keyval [sink key val] (returning sink (.write sink key val)))

  IPersistentCollection
  (-emit-keyval [sink key val] (conj sink [(w/clone key) (w/clone val)])))

(defn wrap-sink
  "Return new tuple sink which wraps keys and values as the types
`ckey` and `cval` respectively."
  [ckey cval sink]
  (let [wkey (w/new-instance ckey)
        wval (w/new-instance cval)]
    (reify TupleSink
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
  "Return new tuple sink which sinks values as `kind`, which may be
one of `:keys`, `:vals`, or `:keyvals`."
  [kind sink] (vary-meta sink ::tuples-as kind))

(defn ^:private emit-fn
  "Tuple-emitting function for `sink`."
  [sink] (-> (meta sink) (get ::tuples-as :keyvals) emit-fn*))

(defn sink
  "Emit all tuples from `coll` to `sink`."
  [sink coll] (r/reduce (emit-fn sink) sink coll))

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
  ([] (make-job))
  ([conf]
     (condp instance? conf
       Job (make-job (-> ^Job conf .getConfiguration Configuration.))
       JobConf (make-job (Configuration. ^JobConf conf))
       #_else (make-job ^Configuration conf))))

(defn mapper!
  "Allocate and return new parkour mapper class for `job` as calling
`var` with `args`."
  [^Job job var & args]
  (let [conf (.getConfiguration job)
        i (.getInt conf "parkour.mapper.next" 0)]
    (doto conf
      (.setInt "parkour.mapper.next" (inc i))
      (.set (format "parkour.mapper.%d.var" i) (pr-str var))
      (.set (format "parkour.mapper.%d.args" i) (pr-str args)))
    (Class/forName (format "parkour.hadoop.Mappers$_%d" i))))

(defn reducer!
  "Allocate and return new parkour reducer class for `job` as calling
`var` with `args`."
  [^Job job var & args]
  (let [conf (.getConfiguration job)
        i (.getInt conf "parkour.reducer.next" 0)]
    (doto conf
      (.setInt "parkour.reducer.next" (inc i))
      (.set (format "parkour.reducer.%d.var" i) (pr-str var))
      (.set (format "parkour.reducer.%d.args" i) (pr-str args)))
    (Class/forName (format "parkour.hadoop.Reducers$_%d" i))))

(defn partitioner!
  [^Job job var & args]
  "Allocate and return parkour partitioner class for `job` as calling
`var` with `args`."
  (let [conf (.getConfiguration job)]
    (doto conf
      (.set "parkour.partitioner.var" (pr-str var))
      (.set "parkour.partitioner.args" (pr-str args)))
    parkour.hadoop.Partitioner))
