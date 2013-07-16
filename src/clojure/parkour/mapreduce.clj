(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [parkour.util :refer [returning]]
            [parkour.reducers :as pr])
  (:import [java.util Comparator]
           [clojure.lang IPersistentCollection]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce
             MapContext ReduceContext TaskInputOutputContext]))

(defprotocol MRSource
  (keyvals [source] "")
  (keys [source] "")
  (vals [source] "")
  (keyvalgroups* [source] [source pred] "")
  (keygroups* [source] [source pred] "")
  (valgroups* [source] [source pred] ""))

(defn keyvalgroups
  ([source] (keyvalgroups* source))
  ([pred source] (keyvalgroups* source pred)))

(defn keygroups
  ([source] (keygroups* source))
  ([pred source] (keygroups* source pred)))

(defn valgroups
  ([source] (valgroups* source))
  ([pred source] (valgroups* source pred)))

(defprotocol MRSink
  (emit-keyval [sink keyval] "")
  (emit-key [sink key] "")
  (emit-val [sink val] ""))

(defmacro ^:private task-reducer*
  [nextf dataf]
  `(reify ccp/CollReduce
     (coll-reduce [this# f#] (ccp/coll-reduce this# f# (f#)))
     (coll-reduce [this# f# init#]
       (loop [state# init#]
         (if-not ~nextf
           state#
           (recur (f# state# ~dataf)))))))

(defmacro ^:private task-reducer
  ([context nextm keym]
     `(task-reducer* (~nextm ~context) (~keym ~context)))
  ([context nextm keym valm]
     `(task-reducer* (~nextm ~context) [(~keym ~context) (~valm ~context)])))

(extend-protocol ccp/CollReduce
  TaskInputOutputContext
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init]
       (loop [state init]
         (if-not (.nextKeyValue this)
           state
           (let [k (.getCurrentKey this)
                 v (.getCurrentValue this)]
             (recur (f state [k v]))))))))

(extend-protocol MRSource
  MapContext
  (keyvals [^MapContext source]
    (task-reducer source .nextKeyValue .getCurrentKey .getCurrentValue))
  (keys [^MapContext source]
    (task-reducer source .nextKeyValue .getCurrentKey))
  (vals [^MapContext source]
    (task-reducer source .nextKeyValue .getCurrentValue))
  (keyvalgroups
    ([source] (pr/keyvalgroups = source))
    ([source pred] (pr/keyvalgroups pred source)))
  (keygroups
    ([source] (pr/keygroups = source))
    ([source pred] (pr/keygroups pred source)))
  (valgroups
    ([source] (pr/valgroups = source))
    ([source pred] (pr/valgroups pred source)))

  ReduceContext
  (keyvals [^ReduceContext source]
    (task-reducer source .nextKeyValue .getCurrentKey .getCurrentValue))
  (keys [^ReduceContext source]
    (task-reducer source .nextKeyValue .getCurrentKey))
  (vals [^ReduceContext source]
    (task-reducer source .nextKeyValue .getCurrentValue))
  (keyvalgroups*
    ([^ReduceContext source]
       (task-reducer source .nextKey .getCurrentKey .getValues))
    ([^ReduceContext source pred]
       (pr/keyvalgroups pred source)))
  (keygroups*
    ([^ReduceContext source]
       (task-reducer source .nextKey .getCurrentKey))
    ([^ReduceContext source pred]
       (pr/keygroups pred source)))
  (valgroups*
    ([^ReduceContext source]
       (task-reducer source .nextKey .getValues))
    ([^ReduceContext source pred]
       (pr/valgroups pred source)))

  IPersistentCollection
  (keyvals [source] source)
  (keys [source] (r/map first source))
  (vals [source] (r/map second source))
  (keyvalgroups*
    ([source] (pr/keyvalgroups = source))
    ([source pred] (pr/keyvalgroups pred source)))
  (keygroups*
    ([source] (pr/keygroups = source))
    ([source pred] (pr/keygroups pred source)))
  (valgroups*
    ([source] (pr/valgroups = source))
    ([source pred] (pr/valgroups pred source))))

(extend-protocol MRSink
  TaskInputOutputContext
  (emit-keyval [^TaskInputOutputContext sink [key val]]
    (returning sink (.write sink key val)))
  (emit-key [^TaskInputOutputContext sink key]
    (returning sink (.write sink key (NullWritable/get))))
  (emit-val [^TaskInputOutputContext sink val]
    (returning sink (.write sink (NullWritable/get) val)))

  IPersistentCollection
  (emit-keyval [sink keyval] (conj sink keyval))
  (emit-key [sink key] (conj sink [key nil]))
  (emit-val [sink val] (conj sink [nil val])))
