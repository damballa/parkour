(ns parkour.example.matrixify
  "Example Parkour job graph translating a graph with arbitrary text-labeled
nodes into an absolute-indexed matrix.  Accepts text-lines of
whitespace-separated `src`, `dst`, and `weight` edges.  Translates `src` nodes
to rows, `dst` nodes to column, and `weight`s to matrix values."
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [transduce.reducers :as tr]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (toolbox :as ptb) (graph :as pg) (tool :as tool)]
            [parkour.io (dseq :as dseq) (text :as text) (avro :as mra)
             ,          (dux :as dux) (dval :as dval)]
            [parkour.util :refer [returning]]
            [abracad.avro :as avro]))

(defn parse-m
  "Parse text input lines into (source, dest, weight) graph edges."
  [coll]
  (r/map (fn [line]
           (let [[row col val] (str/split line #"\s+")
                 val (Double/parseDouble val)]
             [col [row val]]))
         coll))

(defn dim-count-r
  "Perform a parallel count, indexing each key within the reduce task, emitting
data with parallel index (reducer, offset) tuple and final reducer count."
  {::mr/source-as :valgroups, ::mr/sink-as dux/named-keyvals}
  [coll]
  (let [red (conf/get-long mr/*context* "mapred.task.partition" -1)]
    (tr/mapcat-state (fn [i odims-vals]
                       [(inc i) (if (identical? ::finished odims-vals)
                                  [[:counts red i]]
                                  (map (fn [[odim val]]
                                         [:data odim [[red i] val]])
                                       odims-vals))])
                     0 (r/mapcat identity [coll [::finished]]))))

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

(defn absind-m
  "Convert inputs to absolute (row, col, val) matrix entries."
  {::mr/sink-as :keys}
  [c-offsets r-offsets coll]
  (let [c-offsets @c-offsets, r-offsets @r-offsets]
    (r/map (fn [[col [row val]]]
             [(absind r-offsets row) (absind c-offsets col) val])
           coll)))

;; Avro schemas
(def name-value (avro/tuple-schema [:string :double]))
(def long-pair (avro/tuple-schema [:long :long]))
(def index-value (avro/tuple-schema [long-pair :double]))
(def entry (avro/tuple-schema [:long :long :double]))

(defn matrixify
  "Run matrix-ification jobs for `dseq`. Return dseq on final matrix entries."
  [conf dseq]
  (let [[c-data c-counts]
        , (-> (pg/input dseq)
              (pg/map #'parse-m)
              (pg/partition (mra/shuffle [:string name-value]))
              (pg/reduce #'dim-count-r)
              (pg/output :data (mra/dsink [:string index-value])
                         :counts (mra/dsink [:long :long])))
        [r-data r-counts]
        , (-> (pg/input c-data)
              (pg/map #'ptb/identity-t)
              (pg/partition (mra/shuffle [:string index-value]))
              (pg/reduce #'dim-count-r)
              (pg/output :data (mra/dsink [long-pair index-value])
                         :counts (mra/dsink [:long :long])))
        [c-counts r-counts r-data]
        , (-> [c-counts r-counts r-data]
              (pg/execute conf `dim-count))
        c-offsets (dval/edn-dval (offsets c-counts))
        r-offsets (dval/edn-dval (offsets r-counts))]
    (-> (pg/input r-data)
        (pg/map #'absind-m c-offsets r-offsets)
        (pg/output (mra/dsink [entry]))
        (pg/fexecute conf `matrixify))))

(defn tool
  [conf & inpaths]
  (let [indseq (apply text/dseq inpaths)]
    (->> (matrixify conf indseq)
         (tr/each (fn [entry]
                    (println (str/join " " entry)))))))

(defn -main
  [& args] (System/exit (tool/run tool args)))
