(ns parkour.reducers
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour.util :refer [returning]]))

(defn reduce-by
  ([keyfn f coll] (reduce-by keyfn f (f) coll))
  ([keyfn f init coll]
     (reify ccp/CollReduce
       (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
       (coll-reduce [_ f1 init1]
         (let [[prev acc acc1]
               , (r/reduce (fn [[prev acc acc1] x]
                             (let [k (keyfn x)]
                               (if (or (= k prev) (identical? prev ::init))
                                 [k (f acc x) acc1]
                                 [k (f init x) (f1 acc1 acc)])))
                           [::init init init1]
                           coll)]
           (if (identical? ::init prev)
             acc1
             (f1 acc1 acc)))))))

(defn funcall
  ([f] (f))
  ([f x] (f x))
  ([f x y] (f x y))
  ([f x y z] (f x y z))
  ([f x y z & more] (apply f x y z more)))

(defn mjuxt
  [& fs]
  (fn
    ([] (mapv funcall fs))
    ([c1] (mapv funcall fs c1))
    ([c1 c2] (mapv funcall fs c1 c2))
    ([c1 c2 & colls] (apply mapv funcall fs c1 c2 colls))))
