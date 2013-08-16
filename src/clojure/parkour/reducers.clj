(ns parkour.reducers
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour.util :refer [returning]]))

(defn reduce-by
  "Partition `coll` with `keyfn` as per `partition-by`, then reduce
each partition with `f` and optional initial value `init` as per
`r/reduce`."
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
  "Call function `f` with additional arguments."
  ([f] (f))
  ([f x] (f x))
  ([f x y] (f x y))
  ([f x y z] (f x y z))
  ([f x y z & more] (apply f x y z more)))

(defn mjuxt
  "Return a function which calls each function in `fs` on the
position-corresponding values in each argument sequence and returns a
vector of the results."
  [& fs]
  (fn
    ([] (mapv funcall fs))
    ([c1] (mapv funcall fs c1))
    ([c1 c2] (mapv funcall fs c1 c2))
    ([c1 c2 & colls] (apply mapv funcall fs c1 c2 colls))))

(defn arg0
  "Accepts any number of argument and returns the first."
  ([x] x)
  ([x y] x)
  ([x y z] x)
  ([x y z & more] x))

(defn arg1
  "Accepts any number of arguments and returns the second."
  ([x y] y)
  ([x y z] y)
  ([x y z & more] y))
