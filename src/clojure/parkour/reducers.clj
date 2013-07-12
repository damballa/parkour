(ns parkour.reducers
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour.util :refer [returning]]))

(defn keyvalgroups
  "For `reduce`-able `coll` of `[key value]` tuples, return
`reduce`-able which groups keys for which the two-argument `pred`
returns true, yielding the `[key values]` tuple for each sequence of
adjacent identical keys."
  [pred coll]
  (reify ccp/CollReduce
    (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
    (coll-reduce [this f1 init]
      (let [[s k vs] (r/reduce (fn [[s k vs] [k' v]]
                                 (cond (nil? vs) [s k' [v]]
                                       (pred k k') [s k (conj vs v)]
                                       :else [(f1 s [k vs]) k' [v]]))
                               [init nil nil]
                               coll)]
        (if (nil? vs) s (f1 s [k vs]))))))

(defn keygroups
  "For `reduce`-able `coll` of `[key value]` tuples, return
`reduce`-able which groups keys for which the two-argument `pred`
returns true, yielding the `key` for each sequence of adjacent
identical keys."
  [pred coll]
  (r/reducer coll (fn [f]
                    (let [k* (atom ::init)]
                      (fn [ret [k' v]]
                        (let [k @k*]
                          (if (and (not (identical? k ::init)) (pred k k'))
                            ret
                            (returning (f ret k')
                              (reset! k* k')))))))))

(defn valgroups
  "For `reduce`-able `coll` of `[key value]` tuples, return
`reduce`-able which groups keys for which the two-argument `pred`
returns true, yielding the `values` for each sequence of adjacent
identical keys."
  [pred coll]
  (reify ccp/CollReduce
    (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
    (coll-reduce [this f1 init]
      (let [[s k vs] (r/reduce (fn [[s k vs] [k' v]]
                                 (cond (nil? vs) [s k' [v]]
                                       (pred k k') [s k (conj vs v)]
                                       :else [(f1 s vs) k' [v]]))
                               [init nil nil]
                               coll)]
        (if (nil? vs) s (f1 s vs))))))
