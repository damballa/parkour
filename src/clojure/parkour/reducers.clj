(ns parkour.reducers
  (:refer-clojure :exclude [map-indexed reductions])
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour.util :refer [returning]]))

(defn map-indexed
  "Reducers version of `map-indexed`."
  [f coll]
  (r/reducer coll
    (fn [f1]
      (let [i (atom -1)]
        (fn [acc x] (f1 acc (f (swap! i inc) x)))))))

(defn reductions
  "Reducers version of `reductions`."
  [f init coll]
  (r/reducer coll
    (fn [f1]
      (let [sentinel (Object.), state (atom sentinel)]
        (fn [acc x]
          (f1 (if (identical? sentinel @state)
                (f1 acc (reset! state init))
                acc)
              (swap! state f x)))))))

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

(defn group-by+
  "Return a map of the values of applying `f` to each item in `coll` to vectors
of the associated results of applying `g` to each item in coll."
  [f g coll]
  (persistent!
   (reduce (fn [m x]
             (let [k (f x), v (g x)]
               (assoc! m k (-> m (get k []) (conj v)))))
           (transient {})
           coll)))

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
  "Accepts any number of arguments and returns the first."
  ([x] x)
  ([x y] x)
  ([x y z] x)
  ([x y z & more] x))

(defn arg1
  "Accepts any number of arguments and returns the second."
  ([x y] y)
  ([x y z] y)
  ([x y z & more] y))
