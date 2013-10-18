(ns parkour.graph-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (graph :as pg) (mapreduce :as mr) (reducers :as pr)
                     (conf :as conf) (fs :as fs) (wrapper :as w)]
            [parkour.io (text :as text) (seqf :as seqf) (avro :as mra)
                        (dux :as dux)]
            [parkour.util :refer [ignore-errors]])
  (:import [org.apache.hadoop.io Text LongWritable]))

(defn word-count
  [[] [dseq dsink]]
  (let [map-fn (fn [input]
                 (->> input mr/vals
                      (r/mapcat #(str/split % #"[ \t]+"))
                      (r/map #(-> [% 1]))))
        red-fn (fn [input]
                 (->> input mr/keyvalgroups
                      (r/map (fn [[word counts]]
                               [word (r/reduce + 0 counts)]))))]
    (-> (pg/source dseq)
        (pg/remote map-fn red-fn)
        (pg/partition [Text LongWritable])
        (pg/remote red-fn)
        (pg/sink dsink))))

(deftest test-word-distinct
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-distinct-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        largs [(text/dseq inpath) (seqf/dsink Text LongWritable outpath)]
        [result] (pg/execute (conf/ig) #'word-count [] largs)]
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (into {} (r/map w/unwrap-all result))))))

(def key-schema
  {:name "key", :type "record"
   :abracad.reader "vector"
   :fields [{:name "id", :type "long"}
            {:name "tag", :type "long"}]})

(def grouping-schema
  {:name "key", :type "record"
   :abracad.reader "vector"
   :fields [{:name "id", :type "long"}
            {:name "tag", :type "long", :order "ignore"}]})

(def output-schema
  {:name "output", :type "record",
   :abracad.reader "vector"
   :fields [{:name "id", :type "long"}
            {:name "left", :type "string"}
            {:name "right", :type "string"}]})

(defn trivial-join
  [[] [left right dsink]]
  (let [map-fn (fn [tag input]
                 (->> input mr/vals
                      (r/map (fn [line]
                               (let [[key val] (str/split line #"\s")]
                                 [[(Long/parseLong key) tag] val])))))]
    (-> [(-> (pg/source left) (pg/remote (partial map-fn 0)))
         (-> (pg/source right) (pg/remote (partial map-fn 1)))]
        (pg/partition
         (mra/shuffle key-schema :string grouping-schema)
         (fn ^long [[key] _ ^long nparts]
           (-> key hash (mod nparts))))
        (pg/remote
         :hof true
         :context true
         (fn [conf]
           (fn [context input]
             (->> input mr/keyvalgroups
                  (r/mapcat (fn [[[id] vals]]
                              (let [vals (into [] vals)
                                    left (first vals)]
                                (r/map #(-> [id left %]) (rest vals)))))
                  (mr/sink-as :keys)))))
        (pg/sink dsink))))

(deftest test-trivial-join
  (let [leftpath (io/resource "join-left.txt")
        rightpath (io/resource "join-right.txt")
        outpath (fs/path "tmp/join-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        largs [(text/dseq leftpath) (text/dseq rightpath)
               (mra/dsink [output-schema] outpath)]
        [result] (pg/execute (conf/ig) #'trivial-join [] largs)]
    (is (= [[0 "foo" "blue"]
            [0 "foo" "red"]
            [0 "foo" "green"]
            [1 "bar" "blue"]
            [2 "baz" "red"]
            [2 "baz" "green"]]
           (into [] (r/map (comp w/unwrap first) result))))))

(defn multiple-outputs
  [[] [dseq even odd]]
  (-> (pg/source dseq)
      (pg/remote
       (fn [input]
         (->> input mr/vals
              (r/mapcat #(str/split % #"[ \t]+"))
              (r/map #(-> [% 1])))))
      (pg/partition [Text LongWritable])
      (pg/remote
       :raw true
       (fn [context]
         (->> context mr/keyvalgroups
              (r/map (fn [[word counts]]
                       [word (->> counts
                                  (r/map w/unwrap)
                                  (r/reduce + 0)
                                  (LongWritable.))]))
              (r/reduce (fn [_ [word total]]
                          (if (even? (w/unwrap total))
                            (dux/write context :even word total)
                            (dux/write context :odd word total)))
                        nil))))
      (pg/sink-multi
       :even even
       :odd odd)))

(deftest test-multiple-outputs
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        evenpath (fs/path outpath "even")
        oddpath (fs/path outpath "odd")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        largs [(text/dseq inpath)
               (seqf/dsink Text LongWritable evenpath)
               (seqf/dsink Text LongWritable oddpath)]
        [even odd] (pg/execute (conf/ig) #'multiple-outputs [] largs)]
    (is (= {"banana" 2} (into {} (r/map w/unwrap-all even))))
    (is (= {"apple" 3, "carrot" 1} (into {} (r/map w/unwrap-all odd))))))
