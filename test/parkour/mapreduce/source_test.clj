(ns parkour.mapreduce.source-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
             ,       (mapreduce :as mr) (graph :as pg)]
            [parkour.io (dsink :as dsink) (seqf :as seqf)])
  (:import [org.apache.hadoop.io Text LongWritable NullWritable]
           [org.apache.hadoop.mapreduce Mapper]))

(def base-data
  [["a" 1] ["a" 2] ["b" 3] ["c" 4] ["c" 5] ["c" 6] ["d" 7]])

(def input-data
  (if-not (-> (conf/ig) str (.contains "yarn"))
    base-data
    (rseq base-data)))

(defn reducer
  [conf shapef mapf transf]
  (let [shapef (resolve shapef)
        mapf (resolve mapf)
        transf (resolve transf)]
    (fn [context input]
      (->> (shapef input)
           (mapf transf)
           (r/map pr-str)
           (mr/sink-as :keys)))))

(defn run-test-source
  [shapef mapf transf]
  (let [inpath (doto (fs/path "tmp/input") fs/path-delete)
        outpath (doto (fs/path "tmp/output") fs/path-delete)
        dseq (dsink/with-dseq (seqf/dsink [Text LongWritable] inpath)
               input-data)]
    (-> (pg/input dseq)
        (pg/map Mapper)
        (pg/partition [Text LongWritable])
        (pg/reduce #'reducer shapef mapf transf)
        (pg/output (seqf/dsink [Text NullWritable] outpath))
        (pg/execute (conf/ig) "test-source")
        (->> first (r/map (comp read-string w/unwrap first)) (into [])))))

(defn into-vec [coll] (into [] coll))
(defn into-seq [coll] (into [] (seq coll)))
(defn into-vec' [[k coll]] [k (into [] coll)])
(defn into-seq' [[k coll]] [k (into [] (seq coll))])

(deftest test-keys
  (let [expected ["a" "a" "b" "c" "c" "c" "d"]]
    (is (= expected (run-test-source `mr/keys `r/map `identity)))
    (is (= expected (run-test-source `mr/keys `map `identity)))))

(deftest test-vals
  (let [expected [1 2 3 4 5 6 7]]
    (is (= expected (run-test-source `mr/vals `r/map `identity)))
    (is (= expected (run-test-source `mr/vals `map `identity)))))

(deftest test-keyvals
  (let [expected base-data]
    (is (= expected (run-test-source `mr/keyvals `r/map `identity)))
    (is (= expected (run-test-source `mr/keyvals `map `identity)))))

(deftest test-keygroups
  (let [expected ["a" "b" "c" "d"]]
    (is (= expected (run-test-source `mr/keygroups `r/map `identity)))
    (is (= expected (run-test-source `mr/keygroups `map `identity)))))

(deftest test-valgroups
  (let [expected [[1 2] [3] [4 5 6] [7]]]
    (is (= expected (run-test-source `mr/valgroups `r/map `into-vec)))
    (is (= expected (run-test-source `mr/valgroups `r/map `into-seq)))
    (is (= expected (run-test-source `mr/valgroups `map `into-vec)))
    (is (= expected (run-test-source `mr/valgroups `map `into-seq)))))

(deftest test-keyvalgroups
  (let [expected [["a" [1 2]] ["b" [3]] ["c" [4 5 6]] ["d" [7]]]]
    (is (= expected (run-test-source `mr/keyvalgroups `r/map `into-vec')))
    (is (= expected (run-test-source `mr/keyvalgroups `r/map `into-seq')))
    (is (= expected (run-test-source `mr/keyvalgroups `map `into-vec')))
    (is (= expected (run-test-source `mr/keyvalgroups `map `into-seq')))))

(deftest test-keykeyvalgroups
  (let [expected [["a" [["a" 1] ["a" 2]]]
                  ["b" [["b" 3]]]
                  ["c" [["c" 4] ["c" 5] ["c" 6]]]
                  ["d" [["d" 7]]]]]
    (is (= expected (run-test-source `mr/keykeyvalgroups `r/map `into-vec')))
    (is (= expected (run-test-source `mr/keykeyvalgroups `r/map `into-seq')))
    (is (= expected (run-test-source `mr/keykeyvalgroups `map `into-vec')))
    (is (= expected (run-test-source `mr/keykeyvalgroups `map `into-seq')))))

(deftest test-keykeygroups
  (let [expected [["a" ["a" "a"]] ["b" ["b"]] ["c" ["c" "c" "c"]] ["d" ["d"]]]]
    (is (= expected (run-test-source `mr/keykeygroups `r/map `into-vec')))
    (is (= expected (run-test-source `mr/keykeygroups `r/map `into-seq')))
    (is (= expected (run-test-source `mr/keykeygroups `map `into-vec')))
    (is (= expected (run-test-source `mr/keykeygroups `map `into-seq')))))

(deftest test-keysgroups
  (let [expected [["a" "a"] ["b"] ["c" "c" "c"] ["d"]]]
    (is (= expected (run-test-source `mr/keysgroups `r/map `into-vec)))
    (is (= expected (run-test-source `mr/keysgroups `r/map `into-seq)))
    (is (= expected (run-test-source `mr/keysgroups `map `into-vec)))
    (is (= expected (run-test-source `mr/keysgroups `map `into-seq)))))
