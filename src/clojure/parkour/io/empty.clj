(ns parkour.io.empty
  (:require [parkour (mapreduce :as mr)]
            [parkour.io (dseq :as dseq)])
  (:import [parkour.hadoop RecordSeqable]
           [org.apache.hadoop.mapreduce Job]))

(defn ^:private create-splits
  [context]
  [])

(defn ^:private create-recseq
  [split context]
  (throw (ex-info "Impossible!" {:split split})))

(defn dseq
  "A dseq which produces no splits and no records."
  []
  (dseq/dseq
   (fn [^Job job]
     (doto job
       (.setInputFormatClass
        (mr/input-format! job #'create-splits []
                          ,,, #'create-recseq []))))))
