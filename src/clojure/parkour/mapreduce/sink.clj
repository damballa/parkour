(ns parkour.mapreduce.sink
  (:refer-clojure :exclude [key val keys vals reduce])
  (:require [clojure.core :as cc]
            [parkour (conf :as conf) (wrapper :as w)]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce MapContext ReduceContext]))

(defprotocol TupleSink
  "Internal protocol for emitting tuples to a sink."
  (-emit-keyval [sink key val]
    "Emit the tuple pair of `key` and `value` to `sink`.")
  (-key-class [sink]
    "Key class expected by `sink`.")
  (-val-class [sink]
    "Value class expected by `sink`."))

(defn emit-keyval
  "Emit pair of `key` and `val` to `sink` as a complete tuple."
  ([sink [key val]] (returning sink (-emit-keyval sink key val)))
  ([sink key val] (returning sink (-emit-keyval sink key val))))

(defn emit-key
  "Emit `key` to `sink` as the key of a key-only tuple."
  [sink key] (returning sink (-emit-keyval sink key nil)))

(defn emit-val
  "Emit `val` to `sink` as the value of a value-only tuple."
  [sink val] (returning sink (-emit-keyval sink nil val)))

(defn key-class
  "Key class expected by `sink`."
  [sink] (-key-class sink))

(defn val-class
  "Value class expected by `sink`."
  [sink] (-val-class sink))

(extend-protocol TupleSink
  MapContext
  (-key-class [sink] (.getMapOutputKeyClass sink))
  (-val-class [sink] (.getMapOutputValueClass sink))
  (-emit-keyval [sink key val] (.write sink key val))

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
    (.write sink key val)))

(defn wrapper-class
  [c c'] (if (and c (isa? c c')) c c'))

(defn wrap-sink
  "Backing implementation for `mr/wrap-sink`."
  [ckey cval sink]
  (let [conf (conf/ig sink)
        ckey (wrapper-class ckey (key-class sink))
        wkey (w/new-instance conf ckey)
        cval (wrapper-class cval (val-class sink))
        wval (w/new-instance conf cval)]
    (reify
      Configurable (getConf [_] conf)
      w/Wrapper (unwrap [_] (w/unwrap sink))
      TupleSink
      (-key-class [_] ckey)
      (-val-class [_] cval)
      (-emit-keyval [_ key val]
        (let [key (if (instance? ckey key) key (w/rewrap wkey key))
              val (if (instance? cval val) val (w/rewrap wval val))]
          (-emit-keyval sink key val))))))

(def emit-fn*
  "Map from sink-type keyword to tuple-emitting function."
  {:keyvals emit-keyval,
   :keys emit-key,
   :vals emit-val})

(defn emit-fn
  "Tuple-emitting function for `coll`."
  [coll] (-> (meta coll) (get ::tuples-as :keyvals) emit-fn*))
