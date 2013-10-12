(ns parkour.remote.mux
  (:require [parkour (conf :as conf) (wrapper :as w)]
            [parkour.mapreduce.input.multiplex :as mux])
  (:import [org.apache.hadoop.mapreduce Mapper Mapper$Context]))

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
