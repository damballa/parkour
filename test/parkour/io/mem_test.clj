(ns parkour.io.mem-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr) (graph :as pg)]
            [parkour.io (dsink :as dsink) (mem :as mem) (seqf :as seqf)]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text LongWritable]))

(use-fixtures :once th/config-fixture)

(deftest test-input
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]]]
    (is (= records (->> records mem/dseq (into []))))))

(deftest test-job-input
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]]]
    (is (= records
           (-> (pg/input (mem/dseq records))
               (pg/map #'identity)
               (pg/sink (seqf/dsink [Text LongWritable]))
               (pg/fexecute (th/config) "mem-test/test-job-input")
               (->> (into [])))))))
