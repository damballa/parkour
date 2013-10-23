(ns parkour.io.dseq.mapred
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w)]
            [parkour.util :refer [doto-let returning]])
  (:import [org.apache.hadoop.mapred InputFormat RecordReader Reporter]))

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

(defn reducible
  [job klass]
  (let [conf (conf/ig job)
        ^InputFormat ifi (w/new-instance conf klass)
        splits (seq (.getSplits ifi conf 1))]
    (reify
      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [_ f init]
        (let [rra (atom ::initial)]
          (try
            (loop [acc init, splits splits, rr nil, key nil, val nil]
              (if (and rr (.next ^RecordReader rr key val))
                (let [acc (f acc [key val])]
                  (if (reduced? acc)
                    @acc
                    (recur acc splits rr key val)))
                (if (empty? splits)
                  acc
                  (let [split (first splits), splits (rest splits),
                        rr (.getRecordReader ifi split conf Reporter/NULL)
                        key (.createKey rr), val (.createValue rr)]
                    (recur acc splits (swap! rra update-rr rr) key val)))))
            (finally
              (close-rr @rra))))))))
