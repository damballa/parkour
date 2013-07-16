(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour.mapreduce :as mr]
            [parkour.writable :as w]
            [parkour.fs :as fs]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.io IntWritable Text]
           [org.apache.hadoop.mapreduce.lib.input
             TextInputFormat FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output
             TextOutputFormat FileOutputFormat]))

(defn mapper
  {::mr/output [Text IntWritable]}
  [conf]
  (fn [input output]
    (let [word (Text.), one (IntWritable. 1)]
      (->> (mr/vals input)
           (r/map w/unwable)
           (r/mapcat #(str/split % #"\s+"))
           (r/map (fn [s] (returning [word one] (w/wable word s))))
           (r/reduce mr/emit-keyval output)))))

(defn reducer
  [conf]
  {::mr/output [Text IntWritable]}
  (fn [input output]
    (let [total (IntWritable.)]
      (->> (mr/keyvalgroups input)
           (r/map (fn [[word counts]]
                    (returning [word total]
                      (->> (r/map w/unwable counts)
                           (r/reduce + 0)
                           (w/wable total)))))
           (r/reduce mr/emit-keyval output)))))

(defn -main
  [& args]
  (let [[inpath outpath] args, job (mr/job)]
    (doto job
      (mr/set-mapper-var #'mapper)
      (mr/set-combiner-var #'reducer)
      (mr/set-reducer-var #'reducer)
      (.setInputFormatClass TextInputFormat)
      (.setOutputFormatClass TextOutputFormat)
      (FileInputFormat/addInputPath (fs/path inpath))
      (FileOutputFormat/setOutputPath (fs/path outpath)))
    (.waitForCompletion job true)))
