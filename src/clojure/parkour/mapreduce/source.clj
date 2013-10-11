(ns parkour.mapreduce.source
  (:refer-clojure :exclude [key val keys vals reduce])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w)])
  (:import [org.apache.hadoop.conf Configurable]
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

(defn reduce
  "As per `cc/reduce`, but in terms of `TupleSource` protocol.  When
provided, applies `nextf` to `context` to retrieve the next tuple
source for each iteration and `dataf` to retrieve the tuple values
passed to `f`."
  ([context f init]
     (reduce next-keyval keyval f init))
  ([nextf dataf context f init]
     (loop [context context, state init]
       (if-let [context (nextf context)]
         (recur context (f state (dataf context)))
         state))))

(defn reducer
  "Make a tuple source `source` `cc/reduce`able with particular
iteration function `nextf` and extraction function `dataf`."
  [nextf dataf source]
  (reify ccp/CollReduce
    (coll-reduce [this f] (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init] (reduce nextf dataf source f init))))

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
    ([this f init] (reduce this f init))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj]
    (reify
      Configurable
      (getConf [_] (conf/ig wobj))

      TupleSource
      (key [_] (w/unwrap (key wobj)))
      (val [_] (w/unwrap (val wobj)))
      (vals [_] (r/map w/unwrap (vals wobj)))
      (next-keyval [this] (when (next-keyval wobj) this))
      (next-key [this] (when (next-key wobj) this))

      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [this f init] (reduce this f init)))))
