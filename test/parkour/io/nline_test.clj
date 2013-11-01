(ns parkour.io.nline-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dsink :as dsink)]
            [parkour.util :refer [compile-when]]))

(compile-when org.apache.hadoop.mapreduce.lib.input.NLineInputFormat
  (require '[parkour.io (nline :as nline)])
  (deftest test-input
    (let [lines ["foo" "bar" "baz" "quux"], p (fs/path "tmp/nline")]
      (fs/path-delete p)
      (with-open [outw (->> "lines.txt" (fs/path p) io/writer)]
        (doseq [^String line lines] (.write outw line) (.write outw "\n")))
      (is (= lines (->> p (nline/dseq 1) w/unwrap (r/map second) (into [])))))))
