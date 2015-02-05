(ns parkour.io.dseq-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (fs :as fs) (mapreduce :as mr) (wrapper :as w)
                     (conf :as conf)]
            [parkour.io (dseq :as dseq) (dsink :as dsink) (text :as text)]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text]
           [org.apache.hadoop.mapred JobConf]))

(use-fixtures :once th/config-fixture)

(defn mr1-add-path
  [c p]
  (org.apache.hadoop.mapred.FileInputFormat/addInputPath c p))

(defn mr2-add-path
  [c p]
  (org.apache.hadoop.mapreduce.lib.input.FileInputFormat/addInputPath c p))

(def TextInputFormat1
  org.apache.hadoop.mapred.TextInputFormat)

(def TextInputFormat2
  org.apache.hadoop.mapreduce.lib.input.TextInputFormat)

(def input-path
  (-> "word-count-input.txt" io/resource fs/path))

(defn run-test-reducible
  [conf]
  (is (= [[0 "apple"]] (->> (dseq/dseq conf)
                            (r/map w/unwrap-all)
                            (r/take 1)
                            (into []))))
  (is (= ["apple"] (->> (dseq/dseq conf)
                        (r/map (comp str second))
                        (r/take 1)
                        (into [])))))

(deftest test-mapred
  (run-test-reducible
   (doto (JobConf. (conf/ig))
     (.setInputFormat TextInputFormat1)
     (mr1-add-path input-path))))

(deftest test-mapreduce
  (run-test-reducible
   (doto (mr/job)
     (.setInputFormatClass TextInputFormat2)
     (mr2-add-path input-path))))

(defn multi-split-dseq
  []
  (let [root (doto (fs/path "tmp/input") fs/path-delete)]
    (spit (fs/path root "part-0.txt") "apple\n")
    (spit (fs/path root "part-1.txt") "banana\n")
    (spit (fs/path root "part-2.txt") "carrot\n")
    (text/dseq root)))

(deftest test-multi-split-reduce-auto
  (is (= ["apple" "banana" "carrot"]
         (->> (multi-split-dseq) (into []) sort vec))))

(deftest test-multi-split-fold-auto
  (let [dseq (multi-split-dseq)]
    (is (= 1790 (->> dseq (r/mapcat identity) (r/map long) (r/reduce +))))
    (is (= 1790 (->> dseq (r/mapcat identity) (r/map long) (r/fold +))))))

(def ^:dynamic *magic-increment*
  "Magic increment for testing dynamic binding conveyance."
  1)

(deftest test-multi-split-fold-auto-bindings
  (let [dseq (multi-split-dseq)]
    (let [f (fn ([] 0) ([x y] (+ *magic-increment* x y)))]
      (is (not= 1790 (->> dseq (r/mapcat identity) (r/map long) (r/fold f))))
      (binding [*magic-increment* 0]
        (is (= 1790 (->> dseq (r/mapcat identity) (r/map long) (r/fold f))))))))

(deftest test-multi-split-reduce-unwrap
  (with-open [source (dseq/source-for (multi-split-dseq))]
    (is (= ["apple" "banana" "carrot"]
           (->> source (into []) sort vec)))))

(deftest test-multi-split-reduce-raw
  (with-open [source (dseq/source-for (multi-split-dseq) :raw? true)]
    (is (= [(Text. "apple") (Text. "banana") (Text. "carrot")]
           (->> source (r/map w/clone) (into []) sort vec)))))

(deftest test-multi-split-seq
  (with-open [source (dseq/source-for (multi-split-dseq))]
    (is (= ["apple" "banana" "carrot"]
           (->> source seq sort vec)))))

(deftest test-input-paths
  (is (= (map fs/path ["file:foo/bar" "file:baz/quux"])
         (dseq/input-paths (text/dseq "foo/bar" "baz/quux")))))

(deftest test-move!
  (let [src-dseq (dsink/with-dseq (text/dsink)
                   [["apple"] ["carrot"] ["banana"]])
        dst-path (doto (fs/path "file:tmp/dseq-move") fs/path-delete)
        dst-dseq (dseq/move! src-dseq dst-path)]
    (is (= [dst-path] (dseq/input-paths dst-dseq)))
    (is (= ["apple" "carrot" "banana"] (into [] dst-dseq)))))

(deftest test-empty
  (th/with-config
    (let [conf (conf/ig),
          path (fs/path "tmp/empty")
          fs (fs/path-fs conf path)
          _ (.delete fs path true)
          _ (.mkdirs fs path)]
      (is (= [] (into [] (text/dseq path)))))))
