(ns parkour.reducers-test
  (:require [clojure.core.reducers :as r]
            [parkour (reducers :as pr)]
            [clojure.test :refer :all]))

(deftest test-reduce-by
  (testing "Explicit init value"
    (is (= [[1 1 1] [2 2 2] [3 3 3]]
           (->> [1 1 1 2 2 2 3 3 3]
                (pr/reduce-by identity conj [])
                (into []))))
    (is (= [[1 1 1]]
           (->> [1 1 1 2 2 2 3 3 3 4 4 4]
                (pr/reduce-by identity conj [])
                (r/take 1)
                (into []))))
    (is (= [[1 1 1] [2 2 2]]
           (->> [1 1 1 2 2 2 3 3 3 4 4 4]
                (r/take 6)
                (pr/reduce-by identity conj [])
                (into []))))
    (is (= [[1 1 1] [2 2 2]]
           (->> [1 1 1 2 2 2 3 3 3 4 4 4]
                (pr/reduce-by identity conj [])
                (r/take 2)
                (into []))))))
