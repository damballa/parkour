(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)
                     (graph :as pg) (tool :as tool)]
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
  (-> (pg/input dseq)
      (pg/map #'mapper)
      (pg/partition [Text LongWritable])
      (pg/combine #'reducer)
      (pg/reduce #'reducer)
      (pg/output dsink)
      (pg/execute conf "word-count")
      first))

(defn tool
  [conf & args]
  (let [[outpath & inpaths] args
        input (apply text/dseq inpaths)
        output (seqf/dsink [Text LongWritable] outpath)]
    (->> (word-count conf input output)
         w/unwrap (into {})
         prn)))

(defn -main
  [& args] (System/exit (tool/run tool args)))
