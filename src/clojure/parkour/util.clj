(ns parkour.util)

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
