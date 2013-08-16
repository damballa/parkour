(ns parkour.word-count-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour.mapreduce :as mr]
            [parkour.fs :as fs]
            [parkour.avro :as mra]
            [parkour.reducers :as pr]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.mapreduce.lib.input
             TextInputFormat FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(defn mapper
  [conf]
  (mra/task
   (fn [input]
     (->> (mr/vals input)
          (r/mapcat #(str/split % #"\s"))
          (r/map #(-> [% 1]))))))

(defn reducer
  [conf]
  (mra/task
   (fn [input]
     (->> (mr/keyvalgroups input)
          (r/map (pr/mjuxt identity (partial r/reduce +)))))))

(defn run-word-count
  [inpath outpath]
  (let [job (mr/job)]
    (doto job
      (.setMapperClass (mr/mapper! job #'mapper))
      (.setCombinerClass (mr/reducer! job #'reducer))
      (.setReducerClass (mr/reducer! job #'reducer))
      (.setInputFormatClass TextInputFormat)
      (mra/set-map-output :string :long)
      (mra/set-output :string :long)
      (FileInputFormat/addInputPath (fs/path inpath))
      (FileOutputFormat/setOutputPath (fs/path outpath)))
    (.waitForCompletion job true)))

(deftest test-word-count
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)]
    (is (= true (run-word-count inpath outpath)))
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (with-open [adf (->> (fs/path outpath "part-*") (fs/path-glob outfs)
                                first io/file avro/data-file-reader)]
             (->> adf seq (into {})))))))
