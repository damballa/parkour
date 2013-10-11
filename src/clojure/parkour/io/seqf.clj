(ns parkour.io.seqf
  (:require [parkour (conf :as conf) (fs :as fs)]
            [parkour.graph (dseq :as dseq) (dsink :as dsink)])
  (:import [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.input SequenceFileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output SequenceFileOutputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(defn dseq
  [& paths]
  (dseq/dseq
   (fn [^Job job]
     (.setInputFormatClass job SequenceFileInputFormat)
     (doseq [path paths]
       (FileInputFormat/addInputPath job (fs/path path))))))

(defn dsink
  [ckey cval path]
  (dsink/dsink
   (dseq path)
   (fn [^Job job]
     (doto job
       (.setOutputFormatClass SequenceFileOutputFormat)
       (.setOutputKeyClass ckey)
       (.setOutputValueClass cval)
       (FileOutputFormat/setOutputPath (fs/path path))))))
