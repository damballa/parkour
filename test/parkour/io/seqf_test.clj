(ns parkour.io.seqf-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (seqf :as seqf)])
  (:import [org.apache.hadoop.io LongWritable Text]))

;; Just do a round-trip test for now, because I don't feel like fighting with
;; the Java sequence file API.
(deftest test-roundtrip
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]],
        p (fs/path "tmp/seqf")]
    (fs/path-delete p)
    (with-open [out (->> p (seqf/dsink Text LongWritable) dsink/sink-for)]
      (mr/sink out records))
    (is (= records (->> p seqf/dseq w/unwrap (into []))))))
