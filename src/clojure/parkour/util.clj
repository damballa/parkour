(ns parkour.util)

(defmacro returning
  "Evaluates the result of `expr`, then evaluates the forms in
`body` (presumably for side-effects), then returns the result of
`expr`.  AKA `prog1`."
  [expr & body] `(let [value# ~expr] ~@body value#))
