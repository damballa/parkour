(ns parkour.graph.dsink
  (:require [parkour.graph.conf :as pgc])
  (:import [java.io Writer]
           [clojure.lang ArityException IDeref]))

(deftype DSink [dseq step]
  Object
  (toString [this]
    (str "#dsink " (pr-str (pgc/step-map this))))

  pgc/ConfigStep
  (-configure [_ job] (pgc/configure! job step))

  IDeref
  (deref [_] dseq))

(defn dsink
  "Return distributed sink represented by job configuration step
`step` and producing the distributed sequence `dseq` once generated.
Result is a config step which returns `dseq` when called as a
zero-argument function."
  [dseq step] (DSink. dseq step))

(defmethod print-method DSink
  [o ^Writer w] (.write w (str o)))

(defn dsink?
  "True iff `x` is a distributed sink."
  [x] (instance? DSink x))
