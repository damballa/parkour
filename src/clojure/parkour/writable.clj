(ns parkour.writable
  (:require [parkour.util :refer [returning]])
  (:import [java.lang.reflect Constructor]
           [clojure.lang IPersistentVector]
           [org.apache.hadoop.io
            IntWritable LongWritable NullWritable Text Writable]))

(defprotocol Wable
  (-wable [obj] "Wrap `obj` in a type-specific Writable."))

(defprotocol Rewable
  (-rewable [wobj obj] "Mutate Writable `wobj` to wrap `obj`."))

(defn wable
  ([obj] (-wable obj))
  ([wobj obj] (returning wobj (-rewable wobj obj))))

(defprotocol Unwable
  (-unwable [wobj] "Unwrap Writable `wobj` in a type-specific fashion."))

(defn unwable
  [wobj] (-unwable wobj))

(extend-protocol Wable
  Writable          (-wable [obj] obj)
  IPersistentVector (-wable [obj] (mapv wable obj))
  nil               (-wable [obj] (NullWritable/get))
  String            (-wable [obj] (Text. obj))
  Integer           (-wable [obj] (IntWritable. obj))
  Long              (-wable [obj] (LongWritable. obj)))

(extend-protocol Rewable
  NullWritable      (-rewable [wobj obj] #_ pass)
  Text              (-rewable [wobj obj] (.set wobj ^String obj))
  IntWritable       (-rewable [wobj obj] (.set wobj obj))
  LongWritable      (-rewable [wobj obj] (.set wobj obj)))

(extend-protocol Unwable
  Object            (-unwable [wobj] wobj)
  IPersistentVector (-unwable [wobj] (mapv unwable wobj))
  NullWritable      (-unwable [wobj] nil)
  Text              (-unwable [wobj] (.toString wobj))
  IntWritable       (-unwable [wobj] (.get wobj))
  LongWritable      (-unwable [wobj] (.get wobj)))

(defn clone
  [wobj]
  (if-not (instance? Writable wobj)
    wobj
    (wable (-> wobj .getClass .newInstance) (unwable wobj))))
