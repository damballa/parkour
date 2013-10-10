(ns parkour.util)

(defmacro ignore-errors
  "Returns the result of evaluating body, or nil if it throws an exception."
  [& body] `(try ~@body (catch java.lang.Exception _# nil)))

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

(defn var-str
  "String fully-qualified name of a var."
  [v] (subs (pr-str v) 2))

(defn var-symbol
  "Fully-qualified symbol for a var."
  [v] (symbol (var-str v)))
