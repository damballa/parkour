(ns parkour.word-count-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour.mapreduce :as mr]
            [parkour.fs :as fs]
            [parkour.io.avro :as mra]
            [parkour.reducers :as pr]
            [parkour.conf :as conf]
            [parkour.util :refer [returning]]
            [parkour.test-helpers :as th])
  (:import [org.apache.hadoop.mapreduce.lib.input
             TextInputFormat FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(use-fixtures :once th/config-fixture)

(defn wc-mapper
  {::mr/source-as :vals, ::sink-as :keyvals}
  [input]
  (->> (r/mapcat #(str/split % #"\s") input)
       (r/map #(-> [% 1]))))

(defn wc-reducer
  {::mr/source-as :keyvalgroups, ::mr/sink-as :keyvals}
  [input] (r/map (pr/mjuxt identity (partial r/reduce +)) input))

(defn run-word-count
  [inpath outpath]
  (let [job (mr/job)]
    (doto job
      (.setMapperClass (mr/mapper! job #'wc-mapper))
      (.setCombinerClass (mr/reducer! job #'wc-reducer))
      (.setReducerClass (mr/reducer! job #'wc-reducer))
      (.setInputFormatClass TextInputFormat)
      (mra/set-map-output :string :long)
      (mra/set-output :string :long)
      (FileInputFormat/addInputPath (fs/path inpath))
      (FileOutputFormat/setOutputPath (fs/path outpath))
      (conf/assoc! "jobclient.completion.poll.interval" 100))
    (.waitForCompletion job true)))

(deftest test-word-count
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-count-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        success? (run-word-count inpath outpath)
        outfile (->> (fs/path outpath "part-*")
                     (fs/path-glob outfs)
                     first io/file)]
    (is (= true success?))
    (is (not (nil? outfile)))
    (is (= {"apple" 3, "banana" 2, "carrot" 1}
           (with-open [adf (avro/data-file-reader outfile)]
             (->> adf seq (into {})))))))


(defn wd-mapper
  [input]
  (->> (mr/vals input)
       (r/mapcat #(str/split % #"\s"))
       (mr/sink-as :keys)))

(defn wd-reducer
  [input]
  (->> (mr/keygroups input)
       (mr/sink-as :keys)))

(defn run-word-distinct
  [inpath outpath]
  (let [job (mr/job)]
    (doto job
      (.setMapperClass (mr/mapper! job #'wd-mapper))
      (.setCombinerClass (mr/combiner! job #'wd-reducer))
      (.setReducerClass (mr/reducer! job #'wd-reducer))
      (.setInputFormatClass TextInputFormat)
      (mra/set-map-output :string)
      (mra/set-output :string)
      (FileInputFormat/addInputPath (fs/path inpath))
      (FileOutputFormat/setOutputPath (fs/path outpath))
      (th/config))
    (.waitForCompletion job true)))

(deftest test-word-distinct
  (let [inpath (io/resource "word-count-input.txt")
        outpath (fs/path "tmp/word-distinct-output")
        outfs (fs/path-fs outpath)
        _ (.delete outfs outpath true)
        success? (run-word-distinct inpath outpath)
        outfile (->> (fs/path outpath "part-*")
                     (fs/path-glob outfs)
                     first io/file)]
    (is (= true success?))
    (is (not (nil? outfile)))
    (is (= ["apple" "banana" "carrot"]
           (with-open [adf (avro/data-file-reader outfile)]
             (->> adf seq (into [])))))))
