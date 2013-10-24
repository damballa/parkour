(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)
                     (graph :as pg)]
            [parkour.io (text :as text) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn mapper
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1])))))

(defn reducer
  [conf]
  (fn [context input]
    (->> (mr/keyvalgroups input)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)])))))

(defn word-count
  [conf dseq dsink]
  (-> (pg/source dseq)
      (pg/map #'mapper)
      (pg/partition [Text LongWritable])
      (pg/reduce #'reducer)
      (pg/sink dsink)
      (pg/execute (conf/ig) "word-count")))

(defn -main
  [& args]
  (let [[inpath outpath] args
        input (text/dseq inpath)
        output (seqf/dsink Text LongWritable outpath)]
    (->> (word-count (conf/ig) input output)
         first w/unwrap (into {})
         prn)))
