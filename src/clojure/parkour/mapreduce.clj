(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour.writable :as w]
            [parkour.util :refer [returning]]
            [parkour.reducers :as pr])
  (:import [java.util Comparator]
           [clojure.lang IPersistentCollection]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce
             Job MapContext ReduceContext TaskInputOutputContext]))

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
  (emit-keyval [sink keyval] (conj sink (mapv w/clone keyval)))
  (emit-key [sink key] (conj sink [(w/clone key) nil]))
  (emit-val [sink val] (conj sink [nil (w/clone val)])))

(def ^:private job-factory-method?
  (->> Job reflect/type-reflect :members (some #(= 'getInstance (:name %)))))

(defmacro ^:private make-job
  [& args] `(~(if job-factory-method? `Job/getInstance `Job.) ~@args))

(defn job
  {:tag `Job}
  ([] (make-job))
  ([conf]
     (if (instance? Job conf)
       (make-job (-> ^Job conf .getConfiguration Configuration.))
       (make-job ^Configuration conf))))

(defn mapper!
  [^Job job var & args]
  (let [conf (.getConfiguration job)
        i (.getInt conf "parkour.mapper.next" 0)]
    (doto conf
      (.setInt "parkour.mapper.next" (inc i))
      (.set (format "parkour.mapper.%d.var" i) (pr-str var))
      (.set (format "parkour.mapper.%d.args" i) (pr-str args)))
    (Class/forName (format "parkour.hadoop.Mappers$_%d" i))))

(defn reducer!
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
  (let [conf (.getConfiguration job)]
    (doto conf
      (.set "parkour.partitioner.var" (pr-str var))
      (.set "parkour.partitioner.args" (pr-str args)))
    parkour.hadoop.Partitioner))

(defn set-mapper-var
  [^Job job var & args]
  (let [[key val] (-> var meta ::output)]
    (.setMapperClass job (apply mapper! job var args))
    (when key (.setMapOutputKeyClass job key))
    (when val (.setMapOutputValueClass job val))))

(defn set-combiner-var
  [^Job job var & args]
  (.setCombinerClass job (apply reducer! job var args)))

(defn set-reducer-var
  [^Job job var & args]
  (let [[key val] (-> var meta ::output)]
    (.setReducerClass job (apply reducer! job var args))
    (when key (.setOutputKeyClass job key))
    (when val (.setOutputValueClass job val))))
