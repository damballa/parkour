(ns parkour.io.dval-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (reducers :as pr) (util :as pu)
             ,       (toolbox :as ptb)]
            [parkour.io (dval :as dval) (text :as text)]
            [parkour.test-helpers :as th]))

(defn filter-lines-m
  {::mr/source-as :vals, ::mr/sink-as :keys}
  [words lines]
  (let [->words (pr/mpartial str/split #"\s+")
        words? (partial some @words)]
    (r/filter (comp words? ->words) lines)))

(defn filter-lines
  [conf words lines]
  (-> (pg/input lines)
      (pg/map #'filter-lines-m words)
      (pg/output (text/dsink (doto "tmp/output" fs/path-delete)))
      (pg/execute conf "filter-lines")
      (first)))

(defn dval-works?
  [words]
  (let [lines (text/dseq (io/resource "matrixify-input.txt"))
        actual (into #{} (filter-lines (conf/ig) words lines))]
    (= #{"foo  blue  1.0"
         "bar  blue  4.0"
         "baz  red   5.0"}
       actual)))

(deftest test-dvals
  (th/with-config
    (are [dval] (dval-works? dval)
         (dval/load-dval #'pu/edn-slurp [(io/resource "words.edn")])
         (dval/copy-dval #'pu/edn-slurp [(io/resource "words.edn")])
         (dval/edn-dval #{"blue" "baz"}))))

(deftest test-dseq-local
  (th/with-config
    (let [words ["foo" "bar" "baz" "quux"]]
      (are [nper] (= words (into [] (dval/dseq nper (dval/edn-dval words))))
           1 2 3 4))))

(deftest test-dseq-job
  (th/with-config
    (let [words ["foo" "bar" "baz" "quux"]]
      (is (= (into #{} words)
             (-> (pg/input (dval/dseq 1 (dval/edn-dval words)))
                 (pg/map #'ptb/identity-t :default :keys)
                 (pg/output (text/dsink))
                 (pg/fexecute (conf/ig) `test-dseq-job)
                 (->> (into #{}))))))))
