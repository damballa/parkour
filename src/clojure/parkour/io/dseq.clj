(ns parkour.io.dseq
  (:require [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (cstep :as cstep) (wrapper :as w)]
            [parkour.io.dseq (mapred :as mr1) (mapreduce :as mr2)]
            [parkour.util :refer [ignore-errors]])
  (:import [java.io Writer]
           [clojure.lang IObj]))

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

(deftype DSeq [meta step]
  Object
  (toString [this]
    (str "conf=" (-> step cstep/step-map pr-str ignore-errors (or "?"))))

  IObj
  (meta [_] meta)
  (withMeta [_ meta] (DSeq. meta step))

  cstep/ConfigStep
  (-apply! [_ job] (cstep/apply! job step))

  ccp/CollReduce
  (coll-reduce [this f] (ccp/coll-reduce this f (f)))
  (coll-reduce [this f init]
    (r/reduce f init (-> this cstep/apply! reducible)))

  w/Wrapper
  (unwrap [this] (r/map w/unwrap-all this))

  DSeqable
  (-dseq [this] this))

(extend-protocol DSeqable
  nil (-dseq [_] nil)
  Object (-dseq [step] (DSeq. (meta step) step)))

(defn dseq?
  "True iff `x` is a distributed sequence."
  [x] (instance? DSeq true))

(defn dseq
  "Return the distributed sequence represented by dseq-able object or
job configuration step `obj`.  Result is a config step and reducible."
  [obj] (-dseq obj))
