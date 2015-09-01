(ns parkour.io.nline
  (:require [parkour (conf :as conf) (fs :as fs)]
            [parkour.io (dseq :as dseq) (empty :as empty)])
  (:import [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.input NLineInputFormat]))

(defn dseq
  "Distributed sequence of input text file lines, allocating no more than `n`
lines of text per mapper.  Tuples consist of (file offset, text line).
Default source shape is `:vals`."
  [n & paths]
  (if (empty? paths)
    (empty/dseq)
    (dseq/dseq
     (fn [^Job job]
       (doto job
         (.setInputFormatClass NLineInputFormat)
         (NLineInputFormat/setNumLinesPerSplit n)
         (FileInputFormat/setInputPaths (fs/path-array paths))
         (dseq/set-default-shape! :vals))))))
