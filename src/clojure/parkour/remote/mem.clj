(ns parkour.remote.mem
  {:private true}
  (:require [clojure.edn :as edn]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf)]
            [parkour.io (mem :as mem)])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.io Text]
           [org.apache.hadoop.mapreduce InputFormat InputSplit Job]
           [org.apache.hadoop.mapreduce Mapper Mapper$Context]
           [parkour.hadoop IInputFormat IInputSplit IRecordReader]
           [parkour.hadoop Mem$InputFormat Mem$InputSplit ProxyRecordReader]))

(defn input-split*
  ([] (input-split* nil nil))
  ([conf] (input-split* nil nil))
  ([conf iname]
     (reify
       IInputSplit
       (getLength [_] (-> iname mem/input-get count))
       (getLocations [_] (into-array String []))
       (readSplit [_ in] (->> in Text/readString keyword (input-split* conf)))
       (write [_ out] (Text/writeString out (name iname)))
       (deref [_] iname))))

(defn input-split
  ([] (Mem$InputSplit.))
  ([conf] (Mem$InputSplit. (conf/ig conf) (into-array Object [])))
  ([conf & args] (Mem$InputSplit. (conf/ig conf) (into-array Object args))))

(defn record-reader*
  [split context]
  (let [data (-> @split mem/input-get (conj nil) atom)
        total (-> data deref count float)]
    (reify IRecordReader
      (close [_])
      (getCurrentKey [_] (-> @data first first))
      (getCurrentValue [_] (-> @data first second))
      (getProgress [_] (/ (- total (count @data)) total))
      (initialize [_ split context] (record-reader* split context))
      (nextKeyValue [_] (-> data (swap! pop) seq boolean)))))

(defn input-format
  []
  (reify IInputFormat
    (getSplits [_ context]
      [(input-split context (mem/get-iname context))])
    (createRecordReader [_ split context]
      (ProxyRecordReader. (record-reader* split context)))))
