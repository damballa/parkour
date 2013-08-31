(ns parkour.inspect-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
                     (inspect :as pi) (wrapper :as w)])
  (:import [org.apache.hadoop.mapred JobConf]))

(defn mr1-add-path
  [c p] (org.apache.hadoop.mapred.FileInputFormat/addInputPath c p))

(defn mr2-add-path
  [c p] (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath c p))

(def TextInputFormat1
  org.apache.hadoop.mapred.TextInputFormat)

(def TextInputFormat2
  org.apache.hadoop.mapreduce.lib.input.TextInputFormat)

(deftest test-mapred
  (let [inpath (-> "word-count-input.txt" io/resource fs/path)]
    (is (= [0 "apple"] (-> (doto (JobConf.)
                             (.setInputFormat TextInputFormat1)
                             (mr1-add-path inpath))
                           pi/records-seqable
                           first)))
    (is (= "apple" (-> (doto (JobConf.)
                         (.setInputFormat TextInputFormat1)
                         (mr1-add-path inpath))
                       (pi/records-seqable (comp str second))
                       first)))
    (is (= [0 "apple"] (-> (doto (JobConf.)
                             (mr1-add-path inpath))
                           (pi/records-seqable w/unwrap-all TextInputFormat1)
                           first)))
    (is (= [0 "apple"]
           (first (pi/records-seqable
                   (JobConf.) w/unwrap-all TextInputFormat1 inpath))))))

(deftest test-mapreduce
  (let [inpath (-> "word-count-input.txt" io/resource fs/path)]
    (is (= [0 "apple"] (-> (doto (mr/job)
                             (.setInputFormatClass TextInputFormat2)
                             (mr2-add-path inpath))
                           pi/records-seqable
                           first)))
    (is (= "apple" (-> (doto (mr/job)
                         (.setInputFormatClass TextInputFormat2)
                         (mr2-add-path inpath))
                       (pi/records-seqable (comp str second))
                       first)))
    (is (= [0 "apple"] (-> (doto (mr/job)
                             (mr2-add-path inpath))
                           (pi/records-seqable w/unwrap-all TextInputFormat2)
                           first)))
    (is (= [0 "apple"]
           (first (pi/records-seqable
                   (mr/job) w/unwrap-all TextInputFormat2 inpath))))))
