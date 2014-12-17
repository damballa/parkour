(ns parkour.io.range-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (mapreduce :as mr) (graph :as pg)
             ,       (toolbox :as ptb)]
            [parkour.io (avro :as mra) (range :as range)]
            [parkour.test-helpers :as th]))

(deftest test-local
  (th/with-config
    (are [nper] (= (range 10) (into [] (range/dseq nper 10)))
         1 2 3 4 5 6 7 8 9 10 11)
    (is (= (range 5 100 7) (into [] (range/dseq 1 5 100 7))))))

(deftest test-job
  (th/with-config
    (is (= (into #{} (range 10))
           (-> (pg/input (range/dseq 1 10))
               (pg/map #'ptb/identity-t :default :keys)
               (pg/output (mra/dsink ['long]))
               (pg/fexecute (conf/ig) `test-job)
               (->> (into #{})))))))
