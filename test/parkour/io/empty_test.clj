(ns parkour.io.empty-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (mapreduce :as mr) (graph :as pg)
             ,       (toolbox :as ptb)]
            [parkour.io (avro :as mra) (empty :as empty)]
            [parkour.test-helpers :as th]))

(deftest test-local
  (th/with-config
    (is (empty? (into [] (empty/dseq))))))

(deftest test-job
  (th/with-config
    (is (empty? (-> (pg/input (empty/dseq))
                    (pg/map #'ptb/identity-t)
                    (pg/output (mra/dsink ['long]))
                    (pg/fexecute (conf/ig) `test-job)
                    (->> (into [])))))))
