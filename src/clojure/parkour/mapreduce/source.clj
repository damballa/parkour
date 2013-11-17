(ns parkour.mapreduce.source
  (:refer-clojure :exclude [key val keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w)])
  (:import [clojure.lang Seqable]
           [java.io Closeable]
           [java.util Collection]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce MapContext ReduceContext]
           [org.apache.hadoop.mapreduce TaskInputOutputContext]))

(defprotocol TupleSource
  "Internal protocol for iterating over key/value tuples from a source
of such tuples."
  (key [this]
    "Current tuple's key.")
  (val [this]
    "Current tuple's value.")
  (vals [this]
    "Current key's sequence of associated values.")
  (next-keyval [this]
    "Source updated to next key/value tuple, implementation.")
  (next-key [this]
    "Source updated to next distinct key, implementation.")
  (-close [source]
    "Close the source, cleaning up any associated resources."))

(defn source?
  "True iff `x` is a tuple source."
  [x] (satisfies? TupleSource x))

(defn keyval
  "Pair of current tuple's key and value."
  [context] [(key context) (val context)])

(defn keyvals
  "Pair of current tuple's key and sequence of associated values."
  [context] [(key context) (vals context)])

(defn mapping
  "A `seq`able or `reduce`able mapping of `f` for each item in `coll`."
  [f coll]
  (reify
    ccp/CollReduce
    (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
    (coll-reduce [_ f1 init] (r/reduce f1 init (r/map f coll)))

    Seqable
    (seq [_] (map f coll))))

(defn keykeyvals
  "Pair of current tuple's key and sequence of grouped key and value pairs."
  [context] [(key context) (mapping #(-> [(key context) %]) (vals context))])

(defn keykeys
  "Pair of current tuple's key and sequence of grouped keys."
  [context] [(key context) (mapping (fn [_] (key context)) (vals context))])

(defn keys
  "Current grouping key's sequence of grouped keys."
  [context] (mapping (fn [_] (key context)) (vals context)))

(defn source-reduce
  "As per `reduce`, but in terms of the `TupleSource` protocol.  When provided,
applies `nextf` to `source` to retrieve the next tuple source for each iteration
and `dataf` to retrieve the tuple values passed to `f`."
  ([source f init]
     (source-reduce next-keyval keyval source f init))
  ([nextf dataf source f init]
     (loop [source source, state init]
       (let [source (nextf source)]
         (if-not source
           state
           (let [state (f state (dataf source))]
             (if (reduced? state)
               @state
               (recur source state))))))))

(defn source-seq
  "A seq for `source`, in terms of the `TupleSource` protocol.  When provided,
applies `nextf` to `source` to retrieve the next tuple source for each iteration
and `dataf` to retrieve the tuple values passed to `f`."
  ([source]
     (source-seq next-keyval keyval source))
  ([nextf dataf source]
     ((fn step [source]
        (lazy-seq
         (if-let [source (nextf source)]
           (cons (dataf source) (step source)))))
      source)))

(defn reducer
  "Make a tuple source `source` `reduce`able and `seq`able with particular
iteration function `nextf` and extraction function `dataf`."
  [nextf dataf source]
  (reify
    ccp/CollReduce
    (coll-reduce [this f] (ccp/coll-reduce this f (f)))
    (coll-reduce [_ f init] (source-reduce nextf dataf source f init))

    Seqable
    (seq [_] (source-seq nextf dataf source))))

(defn seq-source
  [coll]
  (if-let [coll (seq coll)]
    (reify TupleSource
      (key [_] (nth (first coll) 0))
      (val [_] (nth (first coll) 1))
      (next-keyval [_] (seq-source (rest coll)))
      (-close [_]))))

(extend-protocol TupleSource
  nil
  (next-keyval [_] nil)
  (-close [_])

  Collection
  (next-keyval [this] (seq-source this))
  (-close [_])

  MapContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (next-keyval [this] (if (.nextKeyValue this) this))
  (-close [_])

  ReduceContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (vals [this] (.getValues this))
  (next-keyval [this] (if (.nextKeyValue this) this))
  (next-key [this] (if (.nextKey this) this))
  (-close [_]))

(extend-protocol ccp/CollReduce
  TaskInputOutputContext
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init] (source-reduce this f init))))

(defn unwrap-source
  "Produce \"unwrapper\" for `source`, which unwraps each accessed entry."
  [source]
  (reify
    Configurable
    (getConf [_] (conf/ig source))

    TupleSource
    (key [_] (w/unwrap (key source)))
    (val [_] (w/unwrap (val source)))
    (vals [_] (mapping w/unwrap (vals source)))
    (next-keyval [this] (if (next-keyval source) this))
    (next-key [this] (if (next-key source) this))
    (-close [_] (-close source))

    Closeable
    (close [_] (-close source))

    ccp/CollReduce
    (coll-reduce [this f] (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init] (source-reduce this f init))

    Seqable
    (seq [this] (source-seq this))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj] (unwrap-source wobj)))
