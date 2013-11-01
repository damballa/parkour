(ns parkour.io.mem-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr) (graph :as pg)]
            [parkour.io (dsink :as dsink) (mem :as mem) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable]))

(deftest test-input
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]]]
    (is (= records (->> records mem/dseq w/unwrap (into []))))))

(defn map-identity
  [conf] (fn [context input] input))

(deftest test-job-input
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]]
        p (fs/path "tmp/seqf")]
    (fs/path-delete p)
    (let [[results] (-> (pg/input (mem/dseq records))
                        (pg/map #'map-identity)
                        (pg/sink (seqf/dsink [Text LongWritable] p))
                        (pg/execute (conf/ig) "mem-test/test-job-input"))]
      (is (= records (->> results w/unwrap (into [])))))))
