(ns parkour.mapreduce.source
  (:refer-clojure :exclude [key val keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w)])
  (:import [clojure.lang Seqable]
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
    "Source updated to next key/value tuple.")
  (next-key [this]
    "Source updated to next distinct key."))

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
       (if-let [source (nextf source)]
         (recur source (f state (dataf source)))
         state))))

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

(extend-protocol TupleSource
  MapContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (next-keyval [this] (when (.nextKeyValue this) this))

  ReduceContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (vals [this] (.getValues this))
  (next-keyval [this] (when (.nextKeyValue this) this))
  (next-key [this] (when (.nextKey this) this)))

(extend-protocol ccp/CollReduce
  TaskInputOutputContext
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init] (source-reduce this f init))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj]
    (reify
      Configurable
      (getConf [_] (conf/ig wobj))

      TupleSource
      (key [_] (w/unwrap (key wobj)))
      (val [_] (w/unwrap (val wobj)))
      (vals [_] (mapping w/unwrap (vals wobj)))
      (next-keyval [this] (when (next-keyval wobj) this))
      (next-key [this] (when (next-key wobj) this))

      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [this f init] (source-reduce this f init))

      Seqable
      (seq [this] (source-seq this)))))
