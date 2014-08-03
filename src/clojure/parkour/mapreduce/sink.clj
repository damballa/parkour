(ns parkour.mapreduce.sink
  (:refer-clojure :exclude [key val keys vals reduce])
  (:require [clojure.core :as cc]
            [parkour (conf :as conf) (cser :as cser) (wrapper :as w)]
            [parkour.util :refer [returning]])
  (:import [java.io Closeable]
           [clojure.lang IFn]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce MapContext ReduceContext]))

(defprotocol TupleSink
  "Internal protocol for emitting tuples to a sink."
  (-key-class [sink]
    "Key class expected by `sink`.")
  (-val-class [sink]
    "Value class expected by `sink`.")
  (-emit-keyval [sink key val]
    "Emit the tuple pair of `key` and `value` to `sink`.")
  (-close [sink]
    "Close the sink, flushing any buffered output."))

(declare sink-fn*)

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
  (-close [_])

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
    (.write sink key val))
  (-close [_]))

(defn ^:private wrapper-class
  "Sink key/value class, for provided class `c` and original sink class `c'`.
Return provided class if it is compatible with original class; otherwise, return
the original class."
  [c c'] (if (and c (isa? c c')) c c'))

(defn wrap-sink
  "Backing implementation for `mr/wrap-sink`."
  ([sink] (wrap-sink nil nil sink))
  ([ckey cval sink]
     (let [conf (conf/ig sink), unwrapped (w/unwrap sink)
           ckey (wrapper-class ckey (key-class sink))
           wkey (w/new-instance conf ckey)
           cval (wrapper-class cval (val-class sink))
           wval (w/new-instance conf cval)]
       (reify
         Configurable
         (getConf [_] conf)

         w/Wrapper
         (unwrap [_] unwrapped)

         TupleSink
         (-key-class [_] ckey)
         (-val-class [_] cval)
         (-close [_] (-close sink))
         (-emit-keyval [_ key val]
           (let [key (if (instance? ckey key) key (w/rewrap wkey key))
                 val (if (instance? cval val) val (w/rewrap wval val))]
             (-emit-keyval sink key val)))

         Closeable
         (close [_] (-close sink))

         IFn
         (invoke [sink keyval] (emit-keyval sink keyval))
         (invoke [sink key val] (emit-keyval sink key val))
         (applyTo [sink args]
           (case (count args)
             1 (let [[keyval] args] (emit-keyval sink keyval))
             2 (let [[key val] args] (emit-keyval sink key val))))))))

(defn ^:private output-sink?
  [conf]
  (or (= "reduce" (conf/get conf "parkour.step" "reduce"))
      (zero? (conf/get-int conf "mapred.reduce.tasks" 1))))

(defn ^:private sink-default
  "Sinking function for emitting default results."
  [sink coll]
  (let [shape (if-not (output-sink? sink)
                :keyvals
                (cser/get sink "parkour.sink-as.default" :keyvals))]
    ((sink-fn* shape) sink coll)))

(defn ^:private sink-none
  "Sinking function for emitting no results."
  [sink coll] (cc/reduce (fn [_ _]) nil coll))

(defn ^:private sink-emit-raw
  "Sinking function for basic tuple-emitting function `emit`."
  [emit] (fn [sink coll] (cc/reduce emit sink coll)))

(defn ^:private sink-emit-wrapped
  "Sinking function for wrapping a sink then emitting via function `emit`."
  [emit] (fn [sink coll] (cc/reduce emit (wrap-sink sink) coll)))

(def ^:private sink-fns
  "Map from sink-type keyword to sinking function."
  {:default sink-default
   :none sink-none
   :keyvals (sink-emit-wrapped emit-keyval),
   :keys (sink-emit-wrapped emit-key),
   :vals (sink-emit-wrapped emit-val),
   :keyvals-raw (sink-emit-raw emit-keyval),
   :keys-raw (sink-emit-raw emit-key),
   :vals-raw (sink-emit-raw emit-val),
   })

(defn sink-fn*
  "Tuple-emitting function for keyword or function `f`."
  [f]
  (doto (get sink-fns f f)
    (as-> f (when (keyword? f)
              (throw (ex-info (str "Unknown built-in sink `:" f "`")
                              {:f f}))))))

(defn sink-fn
  "Tuple-emitting function for `coll`."
  [coll] (-> coll meta (get ::sink-as :default) sink-fn*))

(defn sink-as
  "Annotate `coll` as containing values to sink as `kind`."
  [kind coll] (vary-meta coll assoc ::sink-as kind))

(defn maybe-sink-as
  "Annotate `coll` as containing values to sink as `kind`, unless `coll` is
already annotated."
  [kind coll]
  (if (contains? (meta coll) ::sink-as)
    coll
    (sink-as kind coll)))
