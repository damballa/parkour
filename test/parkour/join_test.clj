(ns parkour.join-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr) (reducers :as pr)]
            [parkour.io (dseq :as dseq) (mux :as mux) (avro :as mra)]
            [parkour.util :refer [returning]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.input TextInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]
           [parkour.hadoop Mux$Mapper]))

(use-fixtures :once th/config-fixture)

(defn mapper
  [tag input]
  (->> (mr/vals input)
       (r/map (fn [line]
                (let [[key val] (str/split line #"\s")]
                  [[(Long/parseLong key) tag] val])))))

(defn partitioner
  ^long [[key] _ ^long nparts]
  (-> key hash (mod nparts)))

(defn reducer
  [input]
  (->> (mr/keykeyvalgroups input)
       (r/mapcat (fn [[[id] keyvals]]
                   (let [kv-tag (comp second first), kv-val second
                         vals (pr/group-by+ kv-tag kv-val keyvals)
                         left (get vals 0), right (get vals 1)]
                     (for [left left, right right]
                       [id left right]))))
       (mr/sink-as :keys)))

(defn run-join
  [leftpath rightpath outpath]
  (let [job (mr/job)]
    (doto job
      (mux/add-subconf
       (as-> (mr/job job) job
             (doto job
               (.setInputFormatClass TextInputFormat)
               (FileInputFormat/addInputPath (fs/path leftpath))
               (.setMapperClass (mr/mapper! job #'mapper 0)))))
      (mux/add-subconf
       (as-> (mr/job job) job
             (doto job
               (.setInputFormatClass TextInputFormat)
               (FileInputFormat/addInputPath (fs/path rightpath))
               (.setMapperClass (mr/mapper! job #'mapper 1)))))
      (.setMapperClass Mux$Mapper)
      (mra/set-map-output {:name "key", :type "record"
                           :abracad.reader "vector"
                           :fields [{:name "id", :type "long"}
                                    {:name "tag", :type "long"}]}
                          :string)
      (mra/set-grouping {:name "key", :type "record"
                         :fields [{:name "id", :type "long"}
                                  {:name "tag", :type "long"
                                   :order "ignore"}]})
      (.setPartitionerClass (mr/partitioner! job #'partitioner))
      (.setReducerClass (mr/reducer! job #'reducer))
      (mra/set-output {:name "output", :type "record",
                       :abracad.reader "vector"
                       :fields [{:name "id", :type "long"}
                                {:name "left", :type "string"}
                                {:name "right", :type "string"}]})
      (FileOutputFormat/setOutputPath (fs/path outpath))
      (th/config))
    (.waitForCompletion job true)))

(deftest test-join
  (let [leftpath (io/resource "join-left.txt")
        rightpath (io/resource "join-right.txt")
        outpath (fs/path "tmp/join-output")]
    (with-open [outfs (fs/path-fs outpath)]
      (.delete outfs outpath true))
    (is (= true (run-join leftpath rightpath outpath)))
    (is (= [[0 "foo" "blue"]
            [0 "foo" "green"]
            [0 "foo" "red"]
            [1 "bar" "blue"]
            [2 "baz" "green"]
            [2 "baz" "red"]]
           (->> (mra/dseq [:default] outpath)
                (r/map (comp w/unwrap first))
                (into [])
                sort)))))
