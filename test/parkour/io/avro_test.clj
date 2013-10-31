(ns parkour.io.avro-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (avro :as mra)]))

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
        schema (mra/tuple-schema [:string :long])
        p (fs/path "tmp/avro")]
    (fs/path-delete p)
    (with-open [out (->> p (mra/dsink [schema]) dsink/sink-for)]
      (->> records (mr/sink-as :keys) (mr/sink out)))
    (let [f (->> p fs/path-list (remove fs/hidden?) first io/file)]
      (is (= records (->> p (mra/dseq [:default]) w/unwrap (r/map first)
                          (into [])))))))
