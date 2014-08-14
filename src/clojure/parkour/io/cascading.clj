(ns parkour.io.cascading
  (:require [parkour (conf :as conf) (fs :as fs) (wrapper :as w)]
            [parkour.io (dseq :as dseq) (dsink :as dsink) (seqf :as seqf)]
            [parkour.io.transient :refer [transient-path]]
            [parkour.util :refer [doto-let returning]])
  (:import [cascading.tuple Tuple]))

(extend-type Tuple
  w/Wrapper
  (unwrap [tuple] (-> tuple seq vec))
  (rewrap [tuple v]
    (if (or (.isUnmodifiable tuple) (not= (.size tuple) (count v)))
      (reduce (fn [^Tuple t x] (doto t (.add x))) (Tuple.) v)
      (returning tuple
        (reduce (fn [i x] (returning (inc i) (.set tuple i x))) 0 v)))))

(defn add-serializations
  "Configuration step for adding base Cascading serializations."
  [conf]
  (->> (-> (conf/get-vector conf "io.serializations" [])
           (concat ["cascading.tuple.hadoop.BytesSerialization"
                    "cascading.tuple.hadoop.TupleSerialization"])
           (distinct))
       (conf/assoc! conf "io.serializations")))

(defn dseq
  "Distributed sequence for reading from Cascading sequence files at `paths`."
  [& paths] (dseq/dseq [add-serializations (apply seqf/dseq paths)]))

(defn dsink
  "Distributed sink writing `Tuple`s to Cascading sequence files at `path`, or a
transient path if not provided.  Although both the keys and values are `Tuple`s,
Cascading uses only the sequence file values."
  ([] (dsink (transient-path)))
  ([path]
     (dsink/dsink
      (dseq path)
      [add-serializations
       (seqf/dsink [Tuple Tuple] path)])))
