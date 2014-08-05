(ns parkour.example.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (toolbox :as ptb) (tool :as tool)]
            [parkour.io (text :as text) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count-m
  [coll]
  (->> coll
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))

(defn word-count
  [conf lines]
  (-> (pg/input lines)
      (pg/map #'word-count-m)
      (pg/partition [Text LongWritable])
      (pg/combine #'ptb/keyvalgroups-r #'+)
      (pg/output (seqf/dsink [Text LongWritable]))
      (pg/fexecute conf `word-count)))

(defn tool
  [conf & inpaths]
  (->> (apply text/dseq inpaths)
       (word-count conf)
       (into {})
       (prn)))

(defn -main
  [& args] (System/exit (tool/run tool args)))
