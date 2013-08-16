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
           [clojure.lang IPersistentCollection Indexed Seqable]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapred JobConf]
           [org.apache.hadoop.mapreduce
             Job MapContext ReduceContext TaskInputOutputContext]))

(defprotocol TupleContext
  (tc-key [this])
  (tc-val [this])
  (tc-vals [this])
  (tc-next-keyval [this])
  (tc-next-key [this]))

(defn tc-keyval
  [context] [(tc-key context) (tc-val context)])

(defn tc-keyvals
  [context] [(tc-key context) (tc-vals context)])

(defn tc-reduce
  ([context f init]
     (tc-reduce tc-next-keyval tc-keyval f init))
  ([nextf dataf context f init]
     (loop [context context, state init]
       (if-let [context (nextf context)]
         (recur context (f state (dataf context)))
         state))))

(defn ^:private task-reducer
  [nextf dataf context]
  (reify ccp/CollReduce
    (coll-reduce [this f]
      (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init]
      (tc-reduce nextf dataf context f init))))

(extend-protocol TupleContext
  MapContext
  (tc-key [this] (.getCurrentKey this))
  (tc-val [this] (.getCurrentValue this))
  (tc-next-keyval [this] (when (.nextKeyValue this) this))

  ReduceContext
  (tc-key [this] (.getCurrentKey this))
  (tc-val [this] (.getCurrentValue this))
  (tc-vals [this] (.getValues this))
  (tc-next-keyval [this] (when (.nextKeyValue this) this))
  (tc-next-key [this] (when (.nextKey this) this)))

(extend-protocol ccp/CollReduce
  TaskInputOutputContext
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init] (tc-reduce this f init))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj]
    (reify
      TupleContext
      (tc-key [_] (w/unwrap (tc-key wobj)))
      (tc-val [_] (w/unwrap (tc-val wobj)))
      (tc-vals [_] (r/map w/unwrap (tc-vals wobj)))
      (tc-next-keyval [this] (when (tc-next-keyval wobj) this))
      (tc-next-key [this] (when (tc-next-key wobj) this))

      ccp/CollReduce
      (coll-reduce [this f] (tc-reduce this f (f)))
      (coll-reduce [this f init] (tc-reduce this f init)))))

(defn keys
  [context] (task-reducer tc-next-keyval tc-key context))

(defn vals
  [context] (task-reducer tc-next-keyval tc-val context))

(defn keyvals
  [context] (task-reducer tc-next-keyval tc-keyval context))

(defn keygroups
  [context] (task-reducer tc-next-key tc-key context))

(defn valgroups
  [context] (task-reducer tc-next-key tc-vals context))

(defn keyvalgroups
  [context] (task-reducer tc-next-key tc-keyvals context))

(defprotocol MRSink
  (-emit-keyval [sink key val] ""))

(extend-protocol MRSink
  TaskInputOutputContext
  (-emit-keyval [sink key val] (returning sink (.write sink key val)))

  IPersistentCollection
  (-emit-keyval [sink key val] (conj sink [(w/clone key) (w/clone val)])))

(defn wrap-sink
  [ckey cval sink]
  (let [wkey (w/new-instance ckey)
        wval (w/new-instance cval)]
    (reify MRSink
      (-emit-keyval [sink1 key val]
        (returning sink1
          (let [key (if (instance? ckey key) key (w/rewrap wkey key))
                val (if (instance? cval val) val (w/rewrap wval val))]
            (-emit-keyval sink key val)))))))

(defn emit-keyval
  [sink [key val]] (-emit-keyval sink key val))

(defn emit-key
  [sink key] (-emit-keyval sink key nil))

(defn emit-val
  [sink val] (-emit-keyval sink nil val))

(def emit-fn*
  {:keyvals emit-keyval,
   :keys emit-key,
   :vals emit-val})

(defn sink-as
  [kind sink] (vary-meta sink ::tuples-as kind))

(defn emit-fn
  [sink] (-> (meta sink) (get ::tuples-as :keyvals) emit-fn*))

(defn sink
  [sink coll] (r/reduce (emit-fn sink) sink coll))

(def ^:private job-factory-method?
  (->> Job reflect/type-reflect :members (some #(= 'getInstance (:name %)))))

(defmacro ^:private make-job
  [& args] `(~(if job-factory-method? `Job/getInstance `Job.) ~@args))

(defn job
  {:tag `Job}
  ([] (make-job))
  ([conf]
     (condp instance? conf
       Job (make-job (-> ^Job conf .getConfiguration Configuration.))
       JobConf (make-job (Configuration. ^JobConf conf))
       #_else (make-job ^Configuration conf))))

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
