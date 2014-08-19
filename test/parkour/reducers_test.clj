(ns parkour.reducers-test
  (:require [clojure.core.reducers :as r]
            [parkour (reducers :as pr)]
            [clojure.test :refer :all]))

(deftest test-count
  (are [n coll] (= n (pr/count coll))
       0 nil
       1 (range 1)
       2 (range 2)
       10 (range 10)
       5 [1 2 3 4 5]))

(deftest test-map-indexed
  (is (= [[0 :foo] [1 :bar] [2 :baz]]
         (->> [:foo :bar :baz]
              (pr/map-indexed vector)
              (into [])))))

(deftest test-reductions
  (is (= [[] [0] [0 1] [0 1 2] [0 1 2 3]]
         (->> (range 4)
              (pr/reductions conj [])
              (into [])))))

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
                (into [])))))
  (testing "Init value from monoid"
   (is (= [3 4]
          (->> [1 1 1 2 2 2 3 3 3 4 4 4]
               (r/take 5)
               (pr/reduce-by identity +)
               (into []))))))

(deftest test-distinct-by
  (is (= [[1 :a] [2 :d] [3 :g]]
         (->> [[1 :a] [1 :b] [1 :c]
               [2 :d] [2 :e] [2 :f]
               [3 :g] [3 :h] [3 :i]]
              (pr/distinct-by pr/nth0)
              (into [])))))

(deftest test-distinct
  (is (= [1 2 3]
         (->> [1 1 1 2 2 2 3 3 3]
              (pr/distinct)
              (into [])))))
