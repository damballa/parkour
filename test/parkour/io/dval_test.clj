(ns parkour.io.dval-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (reducers :as pr)]
            [parkour.io (dval :as dval) (text :as text)]
            [parkour.test-helpers :as th]))

(defn filter-lines-m
  {::mr/source-as :vals, ::mr/sink-as :keys}
  [words lines]
  (let [->words (pr/mpartial str/split #"\s+")
        words? (partial some @words)]
    (r/filter (comp words? ->words) lines)))

(defn filter-lines-j
  [conf words lines]
  (-> (pg/input lines)
      (pg/map #'filter-lines-m (dval/edn-dval words))
      (pg/output (text/dsink (doto "tmp/output" fs/path-delete)))
      (pg/execute conf "filter-lines")
      (first)))

(deftest test-edn-dval
  (th/with-config
    (let [words #{"blue" "baz"}
          lines (text/dseq (io/resource "matrixify-input.txt"))
          actual (into #{} (filter-lines-j (conf/ig) words lines))]
      (is (= #{"foo  blue  1.0"
               "bar  blue  4.0"
               "baz  red   5.0"}
             actual)))))
