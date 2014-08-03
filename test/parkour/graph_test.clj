(ns parkour.graph-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (graph :as pg) (mapreduce :as mr) (reducers :as pr)
                     (conf :as conf) (fs :as fs) (wrapper :as w)]
            [parkour.io (text :as text) (seqf :as seqf) (avro :as mra)
                        (dux :as dux) (dsink :as dsink) (mem :as mem)]
            [parkour.util :refer [ignore-errors returning]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text LongWritable]))

(use-fixtures :once th/config-fixture)

(defn word-count-mapper
  [input]
  (let [wc (.getCounter mr/*context* "word-count" "words")]
    (->> input mr/vals
         (r/mapcat #(str/split % #"\s+"))
         (r/map (fn [word]
                  (returning word
                    (.increment wc 1))))
         (r/map #(-> [% 1])))))

(defn word-count-reducer
  [input]
  (->> input mr/keyvalgroups
       (r/map (fn [[word counts]]
                [word (r/reduce + 0 counts)]))))

(defn word-count
  [conf dseq dsink]
  (-> (pg/source dseq)
      (pg/map #'word-count-mapper)
      (pg/partition [Text LongWritable])
      (pg/combine #'word-count-reducer)
      (pg/reduce #'word-count-reducer)
      (pg/sink dsink)
      (pg/execute conf "word-count")))

(deftest test-word-count
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        dseq (text/dseq inpath)
        dsink (seqf/dsink [Text LongWritable] outpath)
        [result] (word-count (th/config) dseq dsink)]
    (is (= 6 (-> (->> result mr/counters-map vals (apply merge))
                 (get "MAP_OUTPUT_RECORDS"))))
    (is (= 6 (-> result mr/counters-map (get-in ["word-count" "words"]))))
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (into {} result)))))

(deftest test-word-count-local
  (let [inpath (doto (fs/path "tmp/word-count-input") fs/path-delete)
        outpath (doto (fs/path "tmp/word-count-output") fs/path-delete)
        dseq (dsink/with-dseq (text/dsink inpath)
               (->> ["apple banana banana" "carrot apple" "apple"]
                    (mr/sink-as :keys)))
        dsink (seqf/dsink [Text LongWritable] outpath)
        [result] (word-count (th/config) dseq dsink)]
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (->> result w/unwrap (into {}))))))

(deftest test-word-count-mem
  (let [outpath (doto (fs/path "tmp/word-count-output") fs/path-delete)
        dseq (mem/dseq [[nil "apple banana banana"]
                        [nil "carrot apple"]
                        [nil "apple"]])
        dsink (seqf/dsink [Text LongWritable] outpath)
        [result] (word-count (th/config) dseq dsink)]
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (->> result w/unwrap (into {}))))))

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

(defn trivial-join-mapper
  [tag input]
  (->> input mr/vals
       (r/map (fn [line]
                (let [[key val] (str/split line #"\s")]
                  [[(Long/parseLong key) tag] val])))))

(defn trivial-join-partitioner
  ^long [[key] _ ^long nparts]
  (-> key hash (mod nparts)))

(defn trivial-join-reducer
  [input]
  (->> input mr/keyvalgroups
       (r/mapcat (fn [[[id] vals]]
                   (let [vals (into [] vals)
                         left (first vals)]
                     (r/map #(-> [id left %]) (rest vals)))))
       (mr/sink-as :keys)))

(defn trivial-join
  [conf left right dsink]
  (-> [(-> (pg/source left) (pg/map #'trivial-join-mapper 0))
       (-> (pg/source right) (pg/map #'trivial-join-mapper 1))]
      (pg/partition (mra/shuffle [key-schema :string grouping-schema])
                    #'trivial-join-partitioner)
      (pg/reduce #'trivial-join-reducer)
      (pg/sink dsink)
      (pg/execute conf "trivial-join")))

(deftest test-trivial-join
  (let [leftpath (io/resource "join-left.txt")
        rightpath (io/resource "join-right.txt")
        outpath (fs/path "tmp/join-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        left (text/dseq leftpath), right (text/dseq rightpath)
        dsink (mra/dsink [output-schema] outpath)
        [result] (trivial-join (th/config) left right dsink)]
    (is (= [[0 "foo" "blue"]
            [0 "foo" "green"]
            [0 "foo" "red"]
            [1 "bar" "blue"]
            [2 "baz" "green"]
            [2 "baz" "red"]]
           (->> result (into []) sort)))))

(defn multiple-outputs-mapper
  [input]
  (->> input mr/vals
       (r/mapcat #(str/split % #"[ \t]+"))
       (r/map #(-> [% 1]))))

(defn multiple-outputs-reducer
  [input]
  (->> input mr/keyvalgroups
       (r/map (fn [[word counts]]
                [word (r/reduce + 0 counts)]))
       (r/map (fn [[word count]]
                (let [oname (if (even? count) :even :odd)]
                  [oname word count])))
       (mr/sink-as dux/named-keyvals)))

(defn multiple-outputs
  [conf dseq even odd]
  (-> (pg/source dseq)
      (pg/map #'multiple-outputs-mapper)
      (pg/partition [Text LongWritable])
      (pg/reduce #'multiple-outputs-reducer)
      (pg/sink :even even, :odd odd)
      (pg/execute conf "multiple-outputs")))

(deftest test-multiple-outputs
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        evenpath (fs/path outpath "even")
        oddpath (fs/path outpath "odd")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        dseq (text/dseq inpath)
        even (seqf/dsink [Text LongWritable] evenpath)
        odd (seqf/dsink [Text LongWritable] oddpath)
        [even odd] (multiple-outputs (th/config) dseq even odd)]
    (is (= {"banana" 2} (into {} (r/map w/unwrap-all even))))
    (is (= {"apple" 3, "carrot" 1} (into {} (r/map w/unwrap-all odd))))))

(deftest test-nil-dseq-dsink
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        dseq (text/dseq inpath)
        dsink [(seqf/dsink [Text LongWritable] outpath)]
        [result] (word-count (th/config) dseq dsink)]
    (is (= nil result))))

(defn bad-mapper
  [input] (throw (ex-info "Exception expected" {:from-task? true})))

(deftest test-local-task-exceptions
  (let [inpath (io/resource "word-count-input.txt")
        outpath (doto "tmp/exception" fs/path-delete)
        dseq (text/dseq inpath)
        dsink (seqf/dsink [Text LongWritable] outpath)
        graph (-> (pg/input dseq)
                  (pg/map #'bad-mapper)
                  (pg/output dsink))
        info (try
               (returning {:from-task? false}
                 (pg/execute graph (th/config) "exception"))
               (catch Exception e
                 (-> e .getCause ex-data)))]
    (is (:from-task? info))))
