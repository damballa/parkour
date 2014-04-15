(ns parkour.io.seqf-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (seqf :as seqf)]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io LongWritable Text]))

(use-fixtures :once th/config-fixture)

;; Just do a round-trip test for now, because I don't feel like fighting with
;; the Java sequence file API.
(deftest test-roundtrip
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]],]
    (is (= records
           (->> (dsink/with-dseq
                  (->> (doto "tmp/seqf" fs/path-delete)
                       (seqf/dsink [Text LongWritable]))
                  records)
                (into []))))
    (is (= records
           (->> (dsink/with-dseq
                  (seqf/dsink [Text LongWritable])
                  records)
                (into []))))))
