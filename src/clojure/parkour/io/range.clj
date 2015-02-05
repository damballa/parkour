(ns parkour.io.range
  (:require [parkour (mapreduce :as mr)]
            [parkour.io (dseq :as dseq)]
            [parkour.util :refer [ruquot]])
  (:import [parkour.hadoop RecordSeqable]
           [org.apache.hadoop.mapreduce Job]))

(defn ^:private create-splits
  "Range splits for provided parameters."
  [context nper start end step]
  (let [sstep (* step nper)]
    (map (fn [start]
           (let [end (min end (+ start sstep))
                 length (-> end (- start) (ruquot step))]
             {:start start, :end end, :step step,
              ::mr/length length}))
         (range start end sstep))))

(defn ^:private create-recseq
  "Record seqable for range split `split`."
  [split context]
  (let [{:keys [start end step], length ::mr/length} split]
    (reify RecordSeqable
      (count [_] length)
      (seq [_] (range start end step))
      (close [_]))))

(defn dseq
  "Distributed sequence for a range of numbers.  The overall sequence is derived
from `start`, `end`, and `step` as per core `range` and with the same defaults.
Each map task will receive `nper` values, the final task receiving fewer if the
length of the number of values is not evenly divisible by `nper`."
  ([nper end] (dseq nper 0 end))
  ([nper start end] (dseq nper start end 1))
  ([nper start end step]
     (dseq/dseq
      (fn [^Job job]
        (doto job
          (.setInputFormatClass
           (mr/input-format! job #'create-splits [nper start end step]
                             ,,, #'create-recseq []))
          (dseq/set-default-shape! :keys))))))
