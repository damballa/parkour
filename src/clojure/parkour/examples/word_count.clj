(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour.mapreduce :as mr]
            [parkour.wrapper :as w]
            [parkour.fs :as fs]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.io IntWritable Text]
           [org.apache.hadoop.mapreduce.lib.input
             TextInputFormat FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output
             TextOutputFormat FileOutputFormat]
           [hippy.hadoop HippyWritable]))

(w/auto-wrapper HippyWritable)

(defn mapper
  {::mr/output [HippyWritable HippyWritable]}
  [conf]
  (fn [input output]
    (->> (mr/vals w/unwrap input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1]))
         (r/map (w/wrap-keyvals HippyWritable))
         (r/reduce mr/emit-keyval output))))

(defn reducer
  [conf]
  {::mr/output [HippyWritable HippyWritable]}
  (fn [input output]
    (->> (mr/keyvalgroups w/unwrap input)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)]))
         (r/map (w/wrap-keyvals HippyWritable))
         (r/reduce mr/emit-keyval output))))

(comment

  (defn mapper
    [input]
    (->> (r/map second input)
         (r/mapcat #(str/split % #"\n"))
         (r/map #(-> [% 1]))))

  (defn reducer
    [input]
    (r/map (fn [[word counts]]
             [word (r/reduce + 0 counts)])
           input))

  (defn word-count-job
    [dseq]
    (->> (r/map second dseq)
         (r/mapcat #(str/split % #"\n"))
         (r/map #(-> [% 1]))
         (mr/group)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)]))))

  )

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
