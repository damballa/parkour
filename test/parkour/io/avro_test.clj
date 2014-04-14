(ns parkour.io.avro-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr) (graph :as pg)]
            [parkour.io (dsink :as dsink) (avro :as mra)]
            [parkour.test-helpers :as th]))

(use-fixtures :once th/config-fixture)

(deftest test-raw-io
  (let [p (fs/path "tmp/raw.avro"), records (vec (range 5))]
    (with-open [dfw (avro/data-file-writer "snappy" 'long p)]
      (doseq [r records] (.append dfw r)))
    (with-open [dfr (avro/data-file-reader p)]
      (is (= records (seq dfr))))))

(deftest test-input
  (let [records ["foo" "bar" "baz" "quux"], schema (avro/parse-schema :string)
        p (fs/path "tmp/avro"), f (io/file p "0.avro")]
    (fs/path-delete p)
    (io/make-parents f)
    (with-open [dfw (avro/data-file-writer schema f)]
      (doseq [record records] (.append dfw record)))
    (is (= records (->> p (mra/dseq [:default]) w/unwrap (r/map first)
                        (into []))))))

(deftest test-key-output
  (let [records ["foo" "bar" "baz" "quux"]
        p (fs/path "tmp/avro")]
    (fs/path-delete p)
    (with-open [out (->> p (mra/dsink [:string]) dsink/sink-for)]
      (->> records (mr/sink-as :keys) (mr/sink out)))
    (let [f (->> p fs/path-list (remove fs/hidden?) first io/file)]
      (is (= records (with-open [dfr (avro/data-file-reader f)]
                       (into [] dfr)))))))

(deftest test-kv-output
  (let [records [["foo" 1] ["bar" 1] ["baz" 1] ["quux" 1]]
        p (fs/path "tmp/avro")]
    (fs/path-delete p)
    (with-open [out (->> p (mra/dsink [:string :long]) dsink/sink-for)]
      (mr/sink out records))
    (let [f (->> p fs/path-list (remove fs/hidden?) first io/file)]
      (is (= records (with-open [dfr (avro/data-file-reader f)]
                       (into [] dfr)))))))

(deftest test-kv-roundtrip
  (let [records [["foo" 1] ["bar" 1] ["baz" 1] ["quux" 1]]
        p (fs/path "tmp/avro")]
    (fs/path-delete p)
    (with-open [out (->> p (mra/dsink [:string :long]) dsink/sink-for)]
      (mr/sink out records))
    (let [f (->> p fs/path-list (remove fs/hidden?) first io/file)]
      (is (= records (->> p (mra/dseq [:default :default]) w/unwrap
                          (into [])))))))

(deftest test-tuple-roundtrip
  (let [records [["foo" 1] ["bar" 1] ["baz" 1] ["quux" 1]]
        schema (avro/tuple-schema [:string :long])
        p (fs/path "tmp/avro")]
    (fs/path-delete p)
    (with-open [out (->> p (mra/dsink [schema]) dsink/sink-for)]
      (->> records (mr/sink-as :keys) (mr/sink out)))
    (let [f (->> p fs/path-list (remove fs/hidden?) first io/file)]
      (is (= records (->> p (mra/dseq [:default]) w/unwrap (r/map first)
                          (into [])))))))

(defn ->keys
  [coll] (->> coll mr/keyvals (mr/sink-as :keys)))

(defn kkg->kvg
  [coll]
  (->> (mr/keykeygroups coll)
       (r/map (fn [[[k] ks]] [k (mapv #(nth % 1) ks)]))
       (mr/sink-as :keyvals)))

(deftest test-grouping
  (let [inpath (doto (fs/path "tmp/input") fs/path-delete)
        dseq (dsink/with-dseq (mra/dsink [:string :long] inpath)
               [["a" 1] ["a" 2] ["b" 3] ["c" 4] ["c" 5]])
        outpath (doto (fs/path "tmp/output") fs/path-delete)
        dsink (mra/dsink [:string {:type "array", :items :long}] outpath)
        schema (avro/tuple-schema [:string :long])
        schema+g (avro/grouping-schema 1 schema)
        [result] (-> (pg/input dseq)
                     (pg/map #'->keys)
                     (pg/partition (mra/shuffle [schema nil schema+g]))
                     (pg/reduce #'kkg->kvg)
                     (pg/output dsink)
                     (pg/execute (th/config) "avro-grouping"))]
    (is (= {"a" [1 2], "b" [3], "c", [4 5]} (into {} result)))))
