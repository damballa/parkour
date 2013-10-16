(ns parkour.graph.dseq
  (:require [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [parkour.conf :as conf]
            [parkour.graph.conf :as pgc]
            [parkour.graph.dseq (mapred :as mr1) (mapreduce :as mr2)])
  (:import [java.io Writer]))

(defprotocol DSeqable
  "Protocol for producing distribute sequence."
  (-dseq [this] "Return distributed sequence for `this`."))

(defn reducible
  ([job]
     (let [klass (or (conf/get-class job "mapreduce.inputformat.class" nil)
                     (conf/get-class job "mapred.input.format.class" nil))]
       (reducible job klass)))
  ([job klass]
     (let [rsf (cond (mr1/input-format? klass) mr1/reducible
                     (mr2/input-format? klass) mr2/reducible)]
       (rsf job klass))))

(deftype DSeq [step]
  Object
  (toString [this]
    (str "#dseq " (pr-str (pgc/step-map this))))

  pgc/ConfigStep
  (-configure [_ job] (pgc/configure! job step))

  ccp/CollReduce
  (coll-reduce [this f] (ccp/coll-reduce this f (f)))
  (coll-reduce [this f init]
    (r/reduce f init (-> this pgc/configure! reducible)))

  DSeqable
  (-dseq [this] this))

(extend-protocol DSeqable
  nil (-dseq [_] nil)
  Object (-dseq [step] (DSeq. step)))

(defn dseq
  "Return the distributed sequence represented by dseq-able object or
job configuration step `obj`.  Result is a config step and reducible."
  [obj] (-dseq obj))

(defmethod print-method DSeq
  [o ^Writer w] (.write w (str o)))

(defn dseq?
  "True iff `x` is a distributed sequence."
  [x] (instance? DSeq true))
