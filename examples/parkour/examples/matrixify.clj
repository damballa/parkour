(ns parkour.examples.matrixify
  "Example Parkour job graph translating a graph with arbitrary text-labeled
nodes into an absolute-indexed matrix.  Accepts text-lines of
whitespace-separated `src`, `dst`, and `weight` edges.  Translates `src` nodes
to rows, `dst` nodes to column, and `weight`s to matrix values."
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (tool :as tool)]
            [parkour.io (dseq :as dseq) (text :as text) (avro :as mra)
             ,          (dux :as dux)]
            [parkour.util :refer [returning]]
            [abracad.avro :as avro])
  (:import [org.apache.hadoop.mapreduce Mapper]))

(defn parse-mapper
  "Parse text input lines into (source, dest, weight) graph edges."
  [input]
  (->> (mr/vals input)
       (r/map (fn [line]
                (let [[row col val] (str/split line #"\s+")
                      val (Double/parseDouble val)]
                  [col [row val]])))
       (mr/sink-as :keyvals)))

(defn dim-count-reducer
  "Perform a parallel count, indexing each key within the reduce task, emitting
data with parallel index (reducer, offset) tuple and final reducer count."
  {::mr/adapter mr/contextfn}
  [conf]
  (fn [context input]
    (let [red (conf/get-long conf "mapred.task.partition" -1)
          dim (->> (mr/keyvalgroups input)
                   (reduce (fn [i [_ odims-vals]]
                             (returning (inc i)
                               (->> odims-vals
                                    (r/map (fn [[odim val]]
                                             [odim [[red i] val]]))
                                    (mr/sink-as (dux/named-keyvals :data))
                                    (mr/sink context))))
                           0))]
      (returning nil
        (dux/write context :counts red dim)))))

(defn offsets
  "Build map of offsets from dseq of counts."
  [dseq]
  (->> dseq (into []) (sort-by first)
       (reductions (fn [[_ t] [i n]]
                     [(inc i) (+ t n)])
                   [0 0])
       (into {})))

(defn absind
  "Use `offsets` to calculate absolute index for combination of `red` and `i`."
  [offsets [red i]] (+ i (offsets red)))

(defn absind-mapper
  "Convert inputs to absolute (row, col, val) matrix entries."
  [c-offsets r-offsets input]
  (->> (mr/keyvals input)
       (r/map (fn [[col [row val]]]
                [(absind r-offsets row) (absind c-offsets col) val]))
       (mr/sink-as :keys)))

;; Avro schemas
(def name-value (avro/tuple-schema [:string :double]))
(def long-pair (avro/tuple-schema [:long :long]))
(def index-value (avro/tuple-schema [long-pair :double]))
(def entry (avro/tuple-schema [:long :long :double]))

(defn matrixify
  "Run matrix-ification jobs for `dseq`, storing output under `workdir` and
returning dseq on final matrix entries."
  [conf workdir dseq]
  (let [c-path (fs/path workdir "c"), r-path (fs/path workdir "r")
        c-data (fs/path c-path "data"), r-data (fs/path r-path "data")
        c-counts (fs/path c-path "counts"), r-counts (fs/path r-path "counts")
        matrix-path (fs/path workdir "matrix")
        [c-data c-counts]
        , (-> (pg/input dseq)
              (pg/map #'parse-mapper)
              (pg/partition (mra/shuffle :string name-value))
              (pg/reduce #'dim-count-reducer)
              (pg/output :data (mra/dsink [:string index-value] c-data)
                         :counts (mra/dsink [:long :long] c-counts)))
        [r-data r-counts]
        , (-> (pg/input c-data)
              (pg/map Mapper)
              (pg/partition (mra/shuffle :string index-value))
              (pg/reduce #'dim-count-reducer)
              (pg/output :data (mra/dsink [long-pair index-value] r-data)
                         :counts (mra/dsink [:long :long] r-counts)))
        [c-counts r-counts r-data]
        , (-> [c-counts r-counts r-data]
              (pg/execute conf "matrixify/dim-count"))
        c-offsets (offsets c-counts), r-offsets (offsets r-counts)]
    (-> (pg/input r-data)
        (pg/map #'absind-mapper c-offsets r-offsets)
        (pg/output (mra/dsink [entry] matrix-path))
        (pg/execute conf "matrixify/absind")
        first)))

(defn tool
  [conf & args]
  (let [[workdir & inpaths] args
        indseq (apply text/dseq inpaths)]
    (->> (matrixify conf workdir indseq)
         (reduce (fn [_ [entry _]]
                   (println (str/join "\t" entry)))
                 nil))))

(defn -main
  [& args] (System/exit (tool/run tool args)))
