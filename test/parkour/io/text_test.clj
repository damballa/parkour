(ns parkour.io.text-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink) (text :as text)]))

(deftest test-input
  (let [lines ["foo" "bar" "baz" "quux"], p (fs/path "tmp/text")]
    (fs/path-delete p)
    (with-open [outw (->> "lines.txt" (fs/path p) io/writer)]
      (doseq [^String line lines] (.write outw line) (.write outw "\n")))
    (is (= lines (->> p text/dseq w/unwrap (r/map second) (into []))))))

(deftest test-output
  (let [lines ["foo" "bar" "baz" "quux"], p (fs/path "tmp/text")]
    (fs/path-delete p)
    (with-open [out (->> p text/dsink dsink/sink-for)]
      (->> lines
           (mr/sink-as :keys)
           (mr/sink out)))
    (is (= lines (->> p fs/path-list (remove fs/hidden?) first
                      slurp str/split-lines)))))
