(ns parkour.graph.dsink
  (:require [parkour.graph (conf :as pgc) (dseq :as dseq)]
            [parkour.util :refer [ignore-errors]])
  (:import [java.io Writer]))

(deftype DSink [dseq step]
  Object
  (toString [this]
    (str "conf=" (-> step pgc/step-map pr-str ignore-errors (or "?"))))

  pgc/ConfigStep
  (-configure [_ job] (pgc/configure! job step))

  dseq/DSeqable
  (-dseq [_] dseq))

(defn dsink?
  "True iff `x` is a distributed sink."
  [x] (instance? DSink x))

(defn dsink
  "Return distributed sink represented by job configuration step `step` and
producing the distributed sequence `dseq` once generated.  Result is a config
step which returns `dseq` when called as a zero-argument function.  When `dseq`
is not provided, and `step` is not already a distributed sink, the resulting
sink produces the `nil` dseq."
  ([step] (if (dsink? step) step (dsink nil step)))
  ([dseq step] (DSink. dseq step)))
