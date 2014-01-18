(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (tool :as tool)]
            [parkour.io (text :as text) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn mapper
  [input]
  (->> (mr/vals input)
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))

(defn reducer
  [input]
  (->> (mr/keyvalgroups input)
       (r/map (fn [[word counts]]
                [word (r/reduce + 0 counts)]))))

(defn word-count
  [conf workdir lines]
  (let [wc-path (fs/path workdir "word-count")
        wc-dsink (seqf/dsink [Text LongWritable] wc-path)]
    (-> (pg/input lines)
        (pg/map #'mapper)
        (pg/partition [Text LongWritable])
        (pg/combine #'reducer)
        (pg/reduce #'reducer)
        (pg/output wc-dsink)
        (pg/execute conf "word-count")
        first)))

(defn tool
  [conf & args]
  (let [[workdir & inpaths] args
        lines (apply text/dseq inpaths)]
    (->> (word-count conf workdir lines) (into {}) prn)))

(defn -main
  [& args] (System/exit (tool/run tool args)))
