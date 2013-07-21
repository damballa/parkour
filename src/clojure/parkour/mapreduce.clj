(ns parkour.mapreduce
  (:refer-clojure :exclude [keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour.wrapper :as w]
            [parkour.util :refer [returning]]
            [parkour.reducers :as pr])
  (:import [java.util Comparator]
           [clojure.lang IPersistentCollection]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce
             Job MapContext ReduceContext TaskInputOutputContext]))

(defprotocol MRSource
  (-keyvals [source] [source f] [source kf vf] "")
  (-keys [source] [source f] "")
  (-vals [source] [source f] "")
  (-keyvalgroups [source] [source f] [source kf vf] "")
  (-keygroups [source] [source f] "")
  (-valgroups [source] [source f] ""))

(defn keyvals
  ([source] (-keyvals source))
  ([f source] (-keyvals source f))
  ([kf vf source] (-keyvals source kf vf)))

(defn keys
  ([source] (-keys source))
  ([f source] (-keys source f)))

(defn vals
  ([source] (-vals source))
  ([f source] (-vals source f)))

(defn keyvalgroups
  ([source] (-keyvalgroups source))
  ([f source] (-keyvalgroups source f))
  ([kf vf source] (-keyvalgroups source kf vf)))

(defn keygroups
  ([source] (-keygroups source))
  ([f source] (-keygroups source f)))

(defn valgroups
  ([source] (-valgroups source))
  ([f source] (-valgroups source f)))

(defprotocol MRSink
  (emit-keyval [sink keyval] "")
  (emit-key [sink key] "")
  (emit-val [sink val] ""))

(defmacro ^:private task-reducer
  [nextf dataf]
  `(reify ccp/CollReduce
     (coll-reduce [this# f#] (ccp/coll-reduce this# f# (f#)))
     (coll-reduce [this# f# init#]
       (loop [state# init#]
         (if-not ~nextf
           state#
           (recur (f# state# ~dataf)))))))

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
  (-keyvals
    ([^MapContext source]
       (task-reducer (.nextKeyValue source) [(.getCurrentKey source)
                                             (.getCurrentValue source)]))
    ([^MapContext source f]
       (task-reducer (.nextKeyValue source) [(f (.getCurrentKey source))
                                             (f (.getCurrentValue source))]))
    ([^MapContext source kf vf]
       (task-reducer (.nextKeyValue source) [(kf (.getCurrentKey source))
                                             (vf (.getCurrentValue source))])))
  (-keys
    ([^MapContext source]
       (task-reducer (.nextKeyValue source) (.getCurrentKey source)))
    ([^MapContext source f]
       (task-reducer (.nextKeyValue source) (f (.getCurrentKey source)))))
  (-vals
    ([^MapContext source]
       (task-reducer (.nextKeyValue source) (.getCurrentValue source)))
    ([^MapContext source f]
       (task-reducer (.nextKeyValue source) (f (.getCurrentValue source)))))

  ReduceContext
  (-keyvals
    ([^ReduceContext source]
       (task-reducer (.nextKeyValue source) [(.getCurrentKey source)
                                             (.getCurrentValue source)]))
    ([^ReduceContext source f]
       (task-reducer (.nextKeyValue source) [(f (.getCurrentKey source))
                                             (f (.getCurrentValue source))]))
    ([^ReduceContext source kf vf]
       (task-reducer (.nextKeyValue source) [(kf (.getCurrentKey source))
                                             (vf (.getCurrentValue source))])))
  (-keys
    ([^ReduceContext source]
       (task-reducer (.nextKeyValue source) (.getCurrentKey source)))
    ([^ReduceContext source f]
       (task-reducer (.nextKeyValue source) (f (.getCurrentKey source)))))
  (-vals
    ([^ReduceContext source]
       (task-reducer (.nextKeyValue source) (.getCurrentValue source)))
    ([^ReduceContext source f]
       (task-reducer (.nextKeyValue source) (f (.getCurrentValue source)))))
  (-keyvalgroups
    ([^ReduceContext source]
       (task-reducer (.nextKey source) [(.getCurrentKey source)
                                        (.getValues source)]))
    ([^ReduceContext source f]
       (task-reducer (.nextKey source) [(f (.getCurrentKey source))
                                        (r/map f (.getValues source))]))
    ([^ReduceContext source kf vf]
       (task-reducer (.nextKey source) [(kf (.getCurrentKey source))
                                        (r/map vf (.getValues source))])))
  (-keygroups
    ([^ReduceContext source]
       (task-reducer (.nextKey source) (.getCurrentKey source)))
    ([^ReduceContext source f]
       (task-reducer (.nextKey source) (f (.getCurrentKey source)))))
  (-valgroups
    ([^ReduceContext source]
       (task-reducer (.nextKey source) (.getValues source)))
    ([^ReduceContext source f]
       (task-reducer (.nextKey source) (r/map f (.getValues source))))))

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
