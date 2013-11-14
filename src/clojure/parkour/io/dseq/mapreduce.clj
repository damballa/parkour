(ns parkour.io.dseq.mapreduce
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (source :as src)]
            [parkour.util :refer [ignore-errors doto-let returning]])
  (:import [clojure.lang Seqable]
           [java.io Closeable]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce Job InputFormat RecordReader]))

(defn input-format?
  [klass] (isa? klass InputFormat))

(defn rr?
  [rr] (instance? RecordReader rr))

(deftype RecordReaderTupleSource
    [^Job job ^InputFormat ifi
     ^:unsynchronized-mutable splits
     ^:unsynchronized-mutable ^RecordReader rr]
  Configurable
  (getConf [_] (conf/ig job))

  src/TupleSource
  (key [_] (.getCurrentKey rr))
  (val [_] (.getCurrentValue rr))
  (-next-keyval [_]
    (if (and rr (or (.nextKeyValue rr) (returning false (.close rr))))
      true
      (if-not (empty? splits)
        (let [split (first splits), tac (mr/tac job)]
          (set! splits (rest splits))
          (set! rr (doto (.createRecordReader ifi split tac)
                     (.initialize split tac)))
          (recur)))))
  (-close [_] (when rr (.close rr)))

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
  (let [^InputFormat ifi (w/new-instance job klass)
        splits (seq (.getSplits ifi job))]
    (RecordReaderTupleSource. job ifi splits nil)))
