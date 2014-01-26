(ns parkour.io.dux-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (graph :as pg) (mapreduce :as mr) (reducers :as pr)
             , (conf :as conf) (fs :as fs) (wrapper :as w)]
            [parkour.io (text :as text) (seqf :as seqf) (avro :as mra)
             , (dux :as dux) (dsink :as dsink) (mem :as mem)]
            [parkour.util :refer [ignore-errors returning]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text LongWritable NullWritable]))

(use-fixtures :once th/config-fixture)

(defn first-letter-mapper
  [input]
  (->> (mr/vals input)
       (r/mapcat #(str/split % #"\s+"))
       (r/map (fn [word]
                [(-> word (nth 0) str) word]))
       (mr/sink-as (dux/prefix-keys :words))))

(defn first-letter
  [conf outpath dseq]
  (-> (pg/input dseq)
      (pg/map #'first-letter-mapper)
      (pg/sink :words (seqf/dsink [Text NullWritable] outpath))
      (pg/execute conf "first-letter")))

(deftest test-first-letter
  (let [inpath (io/resource "word-count-input.txt")
        outpath (doto (fs/path "tmp/first-letter") fs/path-delete)
        conf (th/config)
        [result] (first-letter conf outpath (text/dseq inpath))]
    (is (= #{"apple" "banana" "carrot"} (into #{} (r/map first result))))
    (is (= #{"a-m-00000" "b-m-00000" "c-m-00000"}
           (->> (fs/path-list outpath) (r/remove fs/hidden?)
                (r/map #(-> % fs/path .getName))
                (into #{}))))))


(defn mmo-mapper
  {::mr/adapter mr/contextfn}
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map (fn [word]
                  (returning word
                    (dux/write context :words word nil))))
         (r/map #(-> [% 1]))
         (mr/sink-as (dux/map-output :keyvals)))))

(defn mmo-combiner
  [input]
  (->> (mr/keyvalgroups input)
       (r/map (fn [[word counts]]
                [word (r/reduce + 0 counts)]))
       (mr/sink-as (dux/combine-output :keyvals))))

(defn mmo-reducer
  [input]
  (->> (mmo-combiner input)
       (mr/sink-as (dux/named-keyvals :count))))

(defn mmo-word-count
  [conf dseq words-dsink count-dsink]
  (-> (pg/source dseq)
      (pg/map #'mmo-mapper)
      (pg/partition [Text LongWritable])
      (pg/combine #'mmo-combiner)
      (pg/reduce #'mmo-reducer)
      (pg/sink :words words-dsink, :count count-dsink)
      (pg/execute conf "word-count")))

(deftest test-mmo-word-count
  (let [inpath (io/resource "word-count-input.txt")
        words-path (doto (fs/path "tmp/word-count-words") fs/path-delete)
        count-path (doto (fs/path "tmp/word-count-output") fs/path-delete)
        dseq (text/dseq inpath)
        words-dsink (seqf/dsink Text NullWritable words-path)
        count-dsink (seqf/dsink Text LongWritable count-path)
        conf (th/config)
        [words counts] (mmo-word-count conf dseq words-dsink count-dsink)]
    (is (= #{"apple" "banana" "carrot"} (into #{} (r/map first words))))
    (is (= {"apple" 3, "banana" 2, "carrot" 1} (into {} counts)))))
