(ns parkour.io.dseq.mapred
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w)]
            [parkour.util :refer [doto-let returning]]
            [parkour.mapreduce (source :as src)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang Seqable]
           [java.io Closeable]
           [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.mapred InputFormat RecordReader Reporter]))

(defn input-format?
  [klass] (isa? klass InputFormat))

(defn rr?
  [rr] (instance? RecordReader rr))

(deftype RecordReaderTupleSource
    [^Configuration conf ^InputFormat ifi
     ^:unsynchronized-mutable splits
     ^:unsynchronized-mutable ^RecordReader rr
     ^:unsynchronized-mutable key
     ^:unsynchronized-mutable val]
  Configurable
  (getConf [_] conf)

  src/TupleSource
  (key [_] key)
  (val [_] val)
  (next-keyval [this]
    (if (and rr (or (.next rr key val) (returning false (.close rr))))
      this
      (if-not (empty? splits)
        (let [split (first splits)]
          (set! splits (rest splits))
          (set! rr (.getRecordReader ifi split conf Reporter/NULL))
          (set! key (.createKey rr))
          (set! val (.createValue rr))
          (recur)))))
  (-close [_] (.close rr))

  java.io.Closeable
  (close [this] (src/-close this))

  ccp/CollReduce
  (coll-reduce [this f] (ccp/coll-reduce this f (f)))
  (coll-reduce [this f init] (src/source-reduce this f init))

  Seqable
  (seq [this] (src/source-seq this)))

(defn tuple-source
  {:tag `RecordReaderTupleSource}
  [job klass]
  (let [conf (conf/ig job)
        ^InputFormat ifi (w/new-instance conf klass)
        splits (seq (.getSplits ifi conf 1))]
    (RecordReaderTupleSource. conf ifi splits nil nil nil)))
