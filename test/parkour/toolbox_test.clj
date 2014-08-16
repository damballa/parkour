(ns parkour.toolbox-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (cstep :as cstep)
             ,       (wrapper :as w) (mapreduce :as mr) (toolbox :as ptb)
             ,       (reducers :as pr) (graph :as pg)]
            [parkour.io (text :as text)]
            [parkour.util :refer [ignore-errors returning]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.io Text NullWritable]
           [org.apache.hadoop.mapreduce Job Partitioner]))

(deftest test-by-p
  (th/with-config
    (let [job (mr/job), klass (mr/partitioner! job #'ptb/by-p #'pr/nth1)
          p ^Partitioner (w/new-instance job klass)
          np 10, modn #(mod % np), ->part #(.getPartition p % nil np)
          records (map #(-> [(gensym) %]) (range 100))]
      (is (= (->> records (group-by (comp modn pr/nth1)) vals set)
             (->> records (group-by ->part) vals set))))))

(deftest test-nth0-p
  (th/with-config
    (let [job (mr/job), klass (mr/partitioner! job #'ptb/nth0-p)
          p ^Partitioner (w/new-instance job klass)
          np 10, modn #(mod % np), ->part #(.getPartition p % nil np)
          records (map #(-> [% (gensym)]) (range 100))]
      (is (= (->> records (group-by (comp modn pr/nth0)) vals set)
             (->> records (group-by ->part) vals set))))))

(defn words-m
  {::mr/sink-as :keys}
  [coll] (r/mapcat #(str/split % #"\s+") coll))

(deftest test-identity-t
  (th/with-config
    (is (= #{"apple" "banana" "carrot"}
           (-> (pg/input (text/dseq (io/resource "word-count-input.txt")))
               (pg/map #'words-m)
               (pg/partition [Text NullWritable])
               (pg/combine #'ptb/identity-t :keygroups :keys)
               (pg/output (text/dsink))
               (pg/fexecute (conf/ig) `test-identity-t)
               (->> (into #{})))))))
