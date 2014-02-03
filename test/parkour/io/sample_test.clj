(ns parkour.io.sample-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (text :as text) (sample :as sample)]
            [parkour.test-helpers :as th]))

(use-fixtures :once th/config-fixture)

(deftest test-basic
  (is (= 2 (->> (text/dseq "dev-resources/word-count-input.txt")
                (sample/dseq {:size 7 :n 2})
                (into [])
                count))))
