(ns parkour.util
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.io PushbackReader Writer]))

(defmacro ignore-errors
  "Returns the result of evaluating body, or nil if it throws an exception."
  [& body] `(try ~@body (catch Exception _# nil)))

(defmacro returning
  "Evaluates the result of `expr`, then evaluates the forms in
`body` (presumably for side-effects), then returns the result of
`expr`.  AKA `prog1`."
  [expr & body] `(let [value# ~expr] ~@body value#))

(defmacro doto-let
  "`bindings` => `binding-form` `expr`

Evaluates `expr`, then evaluates `body` with its result bound to the
`binding-form`.  Returns the result of `expr`."
  [bindings & body]
  (let [[bf expr & rest] bindings]
    `(let [value# ~expr]
       (let [~bf value#, ~@rest]
         ~@body
         value#))))

(defn coerce
  "Coerce `x` to be of class `c` by applying `f` to it iff `x` isn't
already an instance of `c`."
  [c f x] (if (instance? c x) x (f x)))

(defn map-vals
  "Return a new map made by mapping `f` over the values of `m`."
  [f m] (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn var-str
  "String fully-qualified name of a var."
  [v] (subs (pr-str v) 2))

(defn var-symbol
  "Fully-qualified symbol for a var."
  [v] (symbol (var-str v)))

(defmacro ^:private prev-swap!*
  [atom apply-f & args]
  `(let [atom# ~atom]
     (loop []
       (let [oldval# @atom#, newval# (~@apply-f oldval# ~@args)]
         (if (compare-and-set! atom# oldval# newval#)
           oldval#
           (recur))))))

(defn prev-swap!
  "Like `swap!`, but return the previous value of `atom`."
  ([atom f] (prev-swap!* atom (f)))
  ([atom f x] (prev-swap!* atom (f) x))
  ([atom f x y] (prev-swap!* atom (f) x y))
  ([atom f x y & args] (prev-swap!* atom (apply f) x y args)))

(defn prev-reset!
  "Like `reset!`, but return the previous value of `atom`."
  [atom newval]
  (loop []
    (let [oldval @atom]
      (if (compare-and-set! atom oldval newval)
        oldval
        (recur)))))

;; Derived from clojure/core/reducers.clj
(defmacro compile-if
  "Evaluate `exp` and if it returns logical true and doesn't error, expand to
`then`.  Else expand to `else` if provided."
  ([exp then] `(compile-if ~exp ~then nil))
  ([exp then else]
     (if (try (eval exp) (catch Throwable _ false))
       `(do ~then)
       `(do ~else))))

(defmacro compile-when
  "Evaluate `exp` and if it returns logical true and doesn't error, expand
`body` forms."
  [exp & body] `(compile-if ~exp (do ~@body) nil))

(defn run-id
  "A likely-unique user- and time-based string."
  []
  (let [user (System/getProperty "user.name")
        time (System/currentTimeMillis)
        rand (rand-int Integer/MAX_VALUE)]
    (str user "-" time "-" rand)))

(defn edn-spit
  "Like `spit`, but emits `content` to `f` as EDN."
  [f content & opts]
  (with-open [^Writer f (apply io/writer f opts)]
    (binding [*out* f]
      (pr content))))

(defn edn-slurp
  "Like `slurp`, but parses content from `f` as EDN."
  [f & opts]
  (with-open [f (PushbackReader. (apply io/reader f opts))]
    (edn/read {:readers *data-readers*} f)))
