(ns parkour.core
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str])
  (:import [org.apache.hadoop.mapreduce ReduceContext TaskInputOutputContext]
           [org.apache.hadoop.mapreduce.lib.output MultipleOutputs]))

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

(defn mr-keys
  [^TaskInputOutputContext context]
  (context-reducer (.nextKeyValue context)
                   (.getCurrentKey context)))

(defn mr-vals
  [^TaskInputOutputContext context]
  (context-reducer (.nextKeyValue context)
                   (.getCurrentValue context)))

(defn mr-keyvalgroups
  [^ReduceContext context]
  (context-reducer (.nextKey context)
                   [(.getCurrentKey context)
                    (.getValues context)]))

(defn mr-keygroups
  [^ReduceContext context]
  (context-reducer (.nextKey context)
                   (.getCurrentKey context)))

(defn mr-valgroups
  [^ReduceContext context]
  (context-reducer (.nextKey context) (.getValues context)))

(defn emit-keyvals
  [^TaskInputOutputContext context coll]
  (r/reduce (fn [_ [k v]] (.write context k v)) nil coll))
