(ns parkour.io.cascading-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (cascading :as casc)]))

(deftest test-roundtrip
  (let [records [["foo" 9] ["bar" 8] ["baz" 7] ["quux" 6]],
        p (fs/path "tmp/casc")]
    (fs/path-delete p)
    (with-open [out (->> p (casc/dsink) dsink/sink-for)]
      (->> records (mr/sink-as :vals) (mr/sink out)))
    (is (= records (->> p casc/dseq w/unwrap (r/map second) (into []))))))
