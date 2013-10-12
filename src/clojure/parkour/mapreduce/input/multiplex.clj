(ns parkour.mapreduce.input.multiplex
  {:private true}
  (:require [clojure.edn :as edn]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (mapreduce :as mr) (wrapper :as w)])
  (:import [clojure.lang IDeref]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io Text]
           [org.apache.hadoop.io.serializer SerializationFactory]
           [org.apache.hadoop.io.serializer Deserializer Serializer]
           [org.apache.hadoop.mapreduce InputFormat InputSplit Job]
           [org.apache.hadoop.mapreduce Mapper Mapper$Context]
           [parkour.hadoop.interfaces IInputFormat IInputSplit]
           [parkour.hadoop.input
             MultiplexInputFormat MultiplexInputSplit MultiplexRecordReader]))

(def ^:private ^:const confs-key
  "parkour.multiplex.confs")

(defn get-subconfs
  "Get sequence of `job` multiplex sub-configuration diffs."
  [job] (or (some->> (conf/get job confs-key) (edn/read-string)) []))

(defn add-subconf
  "Add multiplex input format sub-configuration."
  [^Job job ^Job subconf]
  (if-not (identical? MultiplexInputFormat (.getInputFormatClass subconf))
    (let [diff (conf/diff job subconf), diffs (get-subconfs job)]
      (doto job
        (.setInputFormatClass MultiplexInputFormat)
        (conf/assoc! confs-key (-> diffs (conj diff) pr-str))))
    (reduce #(add-subconf %1 (-> subconf mr/job (conf/merge! %2)))
            job (get-subconfs subconf))))

(defn ^:private serializer
  {:tag `Serializer}
  [conf klass] (-> conf SerializationFactory. (.getSerializer klass)))

(defn ^:private deserializer
  {:tag `Deserializer}
  [conf klass] (-> conf SerializationFactory. (.getDeserializer klass)))

(defn ^:private input-split*
  ([] (input-split* nil nil nil))
  ([conf] (input-split* conf nil nil))
  ([^Configuration conf i ^InputSplit split]
     (reify
       IInputSplit
       (getLength [_] (.getLength split))
       (getLocations [_] (.getLocations split))
       (readSplit [_ in]
         (let [i (.readInt in)
               klass (->> in Text/readString (.getClassByName conf))
               split (-> (doto (deserializer conf klass)
                           (.open in))
                         (.deserialize (w/new-instance conf klass)))]
           (input-split* conf i split)))
       (write [_ out]
         (let [klass (class split)]
           (.writeInt out i)
           (Text/writeString out (.getName klass))
           (doto (serializer conf klass)
             (.open out)
             (.serialize split))))

       IDeref
       (deref [_]
         [i split]))))

(defn ^:private input-split
  ([] (MultiplexInputSplit.))
  ([conf] (MultiplexInputSplit. (conf/ig conf) (into-array Object [])))
  ([conf & args]
     (MultiplexInputSplit. (conf/ig conf) (into-array Object args))))

(defn ^:private subjob
  {:tag `Job}
  [context subconf]
  (-> (conf/clone context) (conf/merge! subconf) (mr/job)))

(defn ^:private input-format
  []
  (reify IInputFormat
    (getSplits [_ context]
      (->> (get-subconfs context)
           (map-indexed vector)
           (r/mapcat (fn [[i subconf]]
                       (let [job (subjob context subconf)
                             klass (.getInputFormatClass job)
                             inform (w/new-instance job klass)]
                         (->> (.getSplits ^InputFormat inform job)
                              (r/map (partial input-split job i))))))
           (into [])))
    (createRecordReader [_ split context]
      (let [[i ^InputSplit split] @split
            subconf (-> context get-subconfs (get i))
            job (subjob context subconf)
            klass (.getInputFormatClass job)
            inform (w/new-instance job klass)
            context (mr/tac job (.getTaskAttemptID context))]
        (MultiplexRecordReader.
         (.createRecordReader ^InputFormat inform split context))))))

(def mapper-class
  parkour.hadoop.input.MultiplexMapper)
