(ns parkour.remote.mux
  (:require [clojure.core.reducers :as r]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.io.mux :as mux])
  (:import [clojure.lang IDeref]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io Text]
           [org.apache.hadoop.io.serializer SerializationFactory]
           [org.apache.hadoop.io.serializer Deserializer Serializer]
           [org.apache.hadoop.mapreduce InputFormat InputSplit Job]
           [org.apache.hadoop.mapreduce Mapper Mapper$Context]
           [parkour.hadoop IInputFormat IInputSplit]
           [parkour.hadoop Mux$InputFormat Mux$InputSplit Mux$RecordReader]))

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
  ([] (Mux$InputSplit.))
  ([conf] (Mux$InputSplit. (conf/ig conf) (into-array Object [])))
  ([conf & args]
     (Mux$InputSplit. (conf/ig conf) (into-array Object args))))

(defn ^:private subjob
  {:tag `Job}
  [context subconf]
  (-> (conf/clone context) (conf/merge! subconf) (mr/job)))

(defn ^:private input-format
  []
  (reify IInputFormat
    (getSplits [_ context]
      (->> (mux/get-subconfs context)
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
            subconf (-> context mux/get-subconfs (get i))
            job (subjob context subconf)
            klass (.getInputFormatClass job)
            inform (w/new-instance job klass)
            context (mr/tac job (.getTaskAttemptID context))]
        (Mux$RecordReader.
         (.createRecordReader ^InputFormat inform split context))))))

(defn mapper
  [conf]
  (fn [^Mapper$Context context]
    (let [i (-> context .getInputSplit deref first)
          subconf (-> context mux/get-subconfs (get i))
          rdiff (-> (doto (conf/clone context)
                      (conf/merge! subconf))
                    (conf/diff context))]
      (try
        (conf/merge! context subconf)
        (let [mapper (->> context .getMapperClass (w/new-instance context))]
          (.run ^Mapper mapper context))
        (finally
          (conf/merge! context rdiff))))))
