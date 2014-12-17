(ns parkour.remote.input
  (:require [parkour (conf :as conf) (cser :as cser) (mapreduce :as mr)]
            [parkour.remote.basic :refer [adapt]]
            [parkour.util :refer [coerce]])
  (:import [parkour.hadoop EdnInputSplit]
           [org.apache.hadoop.mapreduce
            , InputSplit JobContext TaskAttemptContext]))

(defn ->EdnInputSplit
  "Constructor function for `EdnInputSplit` instances."
  [conf value] (EdnInputSplit. conf value))

(defn get-splits
  "Generic Parkour `InputFormat#getSplits()` implementation."
  [id ^JobContext context]
  (let [key (str "parkour.input-format." id)
        svar (cser/get context (str key ".svar"))
        args (cser/get context (str key ".sargs"))
        ->EdnInputSplit (partial ->EdnInputSplit (conf/ig context))
        coerce (partial coerce InputSplit ->EdnInputSplit)]
    (mapv coerce (apply svar context args))))

(defn create-record-reader
  "Generic Parkour `InputFormat#createRecordReader()` implementation."
  [id ^InputSplit split ^TaskAttemptContext context]
  (let [key (str "parkour.input-format." id)
        rvar (cser/get context (str key ".rvar"))
        args (cser/get context (str key ".rargs"))
        f (adapt mr/recseqfn rvar)]
    (apply f split context args)))
