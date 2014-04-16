(ns parkour.reducers
  (:refer-clojure :exclude [map-indexed reductions distinct count first keep])
  (:require [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [transduce.reducers :as tr]
            [parkour.util :refer [returning]])
  (:import [java.util Random]))

(defn map-indexed
  "Reducers version of `map-indexed`."
  [f coll]
  (tr/map-state (fn [i x]
                  [(inc i) (f i x)])
                0 coll))

(defn reductions
  "Reducers version of `reductions`."
  ([f coll] (reductions f (f) coll))
  ([f init coll]
     (let [sentinel (Object.)]
       (->> (r/mapcat identity [coll [sentinel]])
            (tr/map-state (fn [acc x]
                            (if (identical? sentinel x)
                              [nil acc]
                              (let [acc' (f acc x)]
                                [acc' acc])))
                          init)))))

(defn reduce-by
  "Partition `coll` with `keyfn` as per `partition-by`, then reduce
each partition with `f` and optional initial value `init` as per
`r/reduce`."
  ([keyfn f coll]
     (let [sentinel (Object.)]
       (->> (r/mapcat identity [coll [sentinel]])
            (tr/map-state (fn [[k acc] x]
                            (if (identical? sentinel x)
                              [nil acc]
                              (let [k' (keyfn x)]
                                (if (or (= k k') (identical? sentinel k))
                                  [[k' (f acc x)] sentinel]
                                  [[k' (f (f) x)] acc]))))
                          [sentinel (f)])
            (r/remove (partial identical? sentinel)))))
  ([keyfn f init coll]
     (let [f (fn ([] init) ([acc x] (f acc x)))]
       (reduce-by keyfn f coll))))

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

(defn mpartial
  "Return a function which applies `f` to its first argument,
followed by `x`, `y`, etc., followed by any additional arguments."
  ([f] f)
  ([f x]
     (fn
       ([o] (f o x))
       ([o y] (f o x y))
       ([o y & args] (apply f o x y args))))
  ([f x y]
     (fn
       ([o] (f o x y))
       ([o z] (f o x y z))
       ([o z & args] (apply f o x y z args))))
  ([f x y & more]
     (fn
       ([o] (apply f o x y more))
       ([o z] (apply f o x y z more))
       ([o z & args] (apply f o x y z (concat more args))))))

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

(defn nth0
  "Like `first`, but more efficient for indexed collections."
  [coll] (nth coll 0))

(defn nth1
  "Like `second`, but more efficient for indexed collections."
  [coll] (nth coll 1))

(defn nth2
  "Third element from indexed collection `coll`."
  [coll] (nth coll 2))

(defn count
  "Like `count`, but implemented in terms of `reduce`."
  [coll] (reduce (comp inc arg0) 0 coll))

(defn first
  "Like `first`, but implemented in terms of `reduce`."
  [coll] (reduce (comp reduced arg1) nil coll))

(defn ffilter
  "Returns the first item in `coll` for which `(pred item)` is true."
  ([coll] (ffilter identity coll))
  ([pred coll] (reduce (fn [_ x] (if (pred x) (reduced x))) nil coll)))

(def keep
  "Like `keep`, but implemented in terms of `reduce`"
  (comp (partial r/remove nil?) r/map))

(defn distinct-by
  "Remove adjacent duplicate values of `(f x)` for each `x` in `coll`."
  [f coll]
  (let [sentinel (Object.)]
    (->> (r/mapcat identity [coll [sentinel]])
         (tr/map-state (fn [x x']
                         [x' (if (= x x') sentinel x')])
                       sentinel)
         (r/remove (partial identical? sentinel)))))

(defn distinct
  "Remove adjacent duplicate values from `coll`."
  [coll] (distinct-by identity coll))

(defn sample-reservoir
  "Take reservoir sample of size `n` from `coll`, optionally using `Random`
instance `r`.  Must reduce entirety of `coll` prior to returning results, which
will be in random order.  Entire sample will be realized in memory."
  ([n coll] (sample-reservoir (Random.) n coll))
  ([r n coll]
     (nth0
      (reduce (fn [[sample i] x]
                (if (< i n)
                  [(conj sample x) (inc i)]
                  (let [i (inc i), j (.nextInt ^Random r i),
                        sample (if (< j n) (assoc sample j x) sample)]
                    [sample i])))
              [[] 0] coll))))
