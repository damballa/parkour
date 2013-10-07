(ns parkour.graph-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (graph :as pg) (mapreduce :as mr) (reducers :as pr)
                     (conf :as conf) (fs :as fs) (wrapper :as w)]
            [parkour.io (text :as text) (seqf :as seqf)]
            [parkour.util :refer [ignore-errors]])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count
  [[] [dseq dsink]]
  (let [map-fn (fn [input]
                 (->> input mr/vals
                      (r/mapcat #(str/split % #"[ \t]+"))
                      (r/map #(-> [% 1]))))
        red-fn (fn [input]
                 (->> input mr/keyvalgroups
                      (r/map (fn [[word counts]]
                               [word (r/reduce + 0 counts)]))))]
    (-> (pg/source dseq)
        (pg/remote map-fn red-fn)
        (pg/partition [Text LongWritable])
        (pg/remote red-fn)
        (pg/sink dsink))))

(deftest test-word-distinct
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-distinct-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        largs [(text/dseq inpath) (seqf/dsink Text LongWritable outpath)]
        [result] (pg/execute (conf/ig) #'word-count [] largs)]
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (into {} (r/map w/unwrap-all result))))))
