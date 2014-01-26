(ns parkour.mapreduce.adapter-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
             ,       (mapreduce :as mr) (graph :as pg)]
            [parkour.io (text :as text) (avro :as mra)]
            [parkour.test-helpers :as th]))

(use-fixtures :once th/config-fixture)

(defn word-count-mapper
  {::mr/adapter mr/contextfn}
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (mapcat #(str/split % #"\s+"))
         (map #(-> [% 1])))))

(defn word-count-reducer
  {::mr/adapter mr/contextfn}
  [conf]
  (fn [context input]
    (->> (mr/keyvalgroups input)
         (map (fn [[word counts]]
                [word (reduce + 0 counts)])))))

(defn word-count-partitioner
  {::mr/adapter mr/partfn}
  [conf]
  (fn ^long [key val ^long nparts]
    (-> ^String key (.charAt 0) int (mod nparts))))

(deftest test-task-collfn
  (let [inpath (fs/path "dev-resources/word-count-input.txt")
        outpath (doto (fs/path "tmp/output") fs/path-delete)
        [result] (-> (pg/input (text/dseq inpath))
                     (pg/map #'word-count-mapper)
                     (pg/partition (mra/shuffle :string :long)
                                   #'word-count-partitioner)
                     (pg/combine #'word-count-reducer)
                     (pg/reduce #'word-count-reducer)
                     (pg/output (mra/dsink [:string :long] outpath))
                     (pg/execute (th/config) "word-count"))]
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (->> result w/unwrap (into {}))))))
