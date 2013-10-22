(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (mapreduce :as mr) (graph :as pg)]
            [parkour.io (text :as text)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count
  [[] [dseq dsink]]
  (-> (pg/source dseq)
      (pg/remote
       (fn [input]
         (->> input mr/vals
              (r/mapcat #(str/split % #"\s+"))
              (r/map #(-> [% 1])))))
      (pg/partition [Text LongWritable])
      (pg/remote
       (fn [input]
         (->> input mr/keyvalgroups
              (r/map (fn [[word counts]]
                       [word (r/reduce + 0 counts)])))))
      (pg/sink dsink)))

(defn -main
  [& args]
  (let [[inpath outpath] args
        input (text/dseq inpath)
        output (text/dsink outpath)]
    (pg/execute (conf/ig) #'word-count [] [input output])))
