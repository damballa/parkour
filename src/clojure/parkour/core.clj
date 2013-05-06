(ns parkour.core
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [cheshire.core :as json])
  (:import [org.apache.hadoop.io Text NullWritable]
           [org.apache.hadoop.mapreduce ReduceContext TaskInputOutputContext]
           [org.apache.hadoop.mapreduce.lib.output MultipleOutputs]
           [org.apache.avro Schema Schema$Parser]
           [org.apache.avro.generic GenericData GenericData$Record]))

(set! *warn-on-reflection* true)

(defmacro returning
  "Evaluates the result of expr, then evaluates the forms in body (presumably
for side-effects), then returns the result of expr."
  [expr & body] `(let [value# ~expr] ~@body value#))

(defmacro ^:private context-reducer
  [nextf dataf]
  `(reify ccp/CollReduce
     (coll-reduce [this# f#] (ccp/coll-reduce this# f# (f#)))
     (coll-reduce [this# f# init#]
       (loop [state# init#]
         (if-not ~nextf
           state#
           (recur (f# state# ~dataf)))))))

(defn mr-keyvals
  [^TaskInputOutputContext context]
  (context-reducer (.nextKeyValue context)
                   [(.getCurrentKey context)
                    (.getCurrentValue context)]))

(defn mr-keyvalgroups
  [^ReduceContext context]
  (context-reducer (.nextKey context)
                   [(.getCurrentKey context)
                    (.getValues context)]))

(defn emit-keyvals
  [^TaskInputOutputContext context coll]
  (r/reduce (fn [_ [k v]] (.write context k v)) nil coll))

