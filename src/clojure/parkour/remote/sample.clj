(ns parkour.remote.sample
  (:require [parkour (conf :as conf) (wrapper :as w)])
  (:import [java.util ArrayList Collections Random]
           [org.apache.hadoop.mapreduce InputFormat]
           [parkour.hadoop IInputFormat]
           [parkour.hadoop Sample$InputFormat]))

(defn input-format
  []
  (reify IInputFormat
    (getSplits [_ context]
      (let [klass (conf/get-class context "parkour.sample.class" nil)
            inform ^InputFormat (w/new-instance context klass)
            n (conf/get-long context "parkour.sample.n" 5)
            seed (conf/get-long context "parkour.sample.seed" 1)
            rnd (Random. seed)]
        (-> (.getSplits inform context) ArrayList.
            (doto (Collections/shuffle rnd))
            (->> (take n)))))
    (createRecordReader [_ split context]
      (let [klass (conf/get-class context "parkour.sample.class" nil)
            inform ^InputFormat (w/new-instance context klass)]
        (.createRecordReader inform split context)))))
