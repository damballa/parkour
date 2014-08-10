(ns parkour.toolbox
  "Utility collection of common task functions."
  (:require [clojure.core.reducers :as r]
            [parkour (mapreduce :as mr) (reducers :as pr) (graph :as pg)]
            [parkour.io (avro :as mra)]))

(defn by-p
  "Partitioner for partitioning by the result of applying a provided function
`f` to each tuple key."
  {::mr/adapter mr/partfn}
  [_ f]
  (fn ^long [k _ ^long nparts]
    (let [^Object o (f k)]
      (-> o .hashCode (mod nparts)))))

(defn nth0-p
  "Partitioner for partitioning on initial element of key."
  ^long [k _ ^long nparts]
  (let [^Object o (pr/nth0 k)]
    (-> o .hashCode (mod nparts))))

(defn identity-t
  "Identity task function.  Accepts optional `in-shape` and `out-shape`
tuple-shaping functions or keywords, using `:default` if not provided.  Applies
input and output shapes to `coll`."
  {::mr/source-as identity}
  ([coll] (identity-t :default :default coll))
  ([in-shape out-shape coll]
     (->> coll (mr/source-as in-shape) (mr/sink-as out-shape))))

(defn keyvalgroups-r
  "Expect `coll` to consist of key/val-group pairs.  Yield tuples of each key
and the result of `reduce`ing the val-group with `f` and optional `init`."
  {::mr/source-as :keyvalgroups}
  ([f coll] (r/map (fn [[k vs]] [k (reduce f vs)]) coll))
  ([f init coll] (r/map (fn [[k vs]] [k (reduce f init vs)]) coll)))
