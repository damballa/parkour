(ns parkour.io.dseq.mapreduce
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [parkour (mapreduce :as mr) (wrapper :as w)]
            [parkour.util :refer [ignore-errors doto-let returning]])
  (:import [org.apache.hadoop.mapreduce InputFormat RecordReader]))

(defn input-format?
  [klass] (isa? klass InputFormat))

(defn ^:private close-rr
  [rr]
  (returning nil
    (when (instance? RecordReader rr)
      (.close ^RecordReader rr))))

(defn ^:private update-rr
  [rr ^RecordReader rr']
  (if (nil? rr)
    (returning nil (.close rr'))
    (returning rr' (close-rr rr))))

(defn ^:private rr-tuple
  [^RecordReader rr]
  [(.getCurrentKey rr) (.getCurrentValue rr)])

(defn reducible
  [job klass]
  (let [^InputFormat ifi (w/new-instance job klass)
        splits (seq (.getSplits ifi job))]
    (reify
      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [_ f init]
        (let [rra (atom ::initial)]
          (try
            (loop [acc init, splits splits, rr nil]
              (if (and rr (.nextKeyValue ^RecordReader rr))
                (let [acc (->> rr rr-tuple (f acc))]
                  (if (reduced? acc)
                    @acc
                    (recur acc splits rr)))
                (if (empty? splits)
                  acc
                  (let [split (first splits), splits (rest splits)
                        tac (mr/tac job),
                        rr (doto (.createRecordReader ifi split tac)
                             (.initialize split tac))]
                    (recur acc splits (swap! rra update-rr rr))))))
            (finally
              (close-rr @rra))))))))
