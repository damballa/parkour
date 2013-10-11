(ns parkour.inspect-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (fs :as fs) (mapreduce :as mr) (wrapper :as w)]
            [parkour.graph.dseq :as dseq])
  (:import [org.apache.hadoop.mapred JobConf]))

(defn mr1-add-path
  [c p]
  (org.apache.hadoop.mapred.FileInputFormat/addInputPath c p))

(defn mr2-add-path
  [c p]
  (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath c p))

(def TextInputFormat1
  org.apache.hadoop.mapred.TextInputFormat)

(def TextInputFormat2
  org.apache.hadoop.mapreduce.lib.input.TextInputFormat)

(def input-path
  (-> "word-count-input.txt" io/resource fs/path))

(defn run-test-reducible
  [conf]
  (is (= [[0 "apple"]] (->> (dseq/reducible conf)
                            (r/map w/unwrap-all)
                            (r/take 1)
                            (into []))))
  (is (= ["apple"] (->> (dseq/reducible conf)
                        (r/map (comp str second))
                        (r/take 1)
                        (into [])))))

(deftest test-mapred
  (run-test-reducible
   (doto (JobConf.)
     (.setInputFormat TextInputFormat1)
     (mr1-add-path input-path))))

(deftest test-mapreduce
  (run-test-reducible
   (doto (mr/job)
     (.setInputFormatClass TextInputFormat2)
     (mr2-add-path input-path))))
