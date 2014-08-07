(ns parkour.io.dseq.mapreduce
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [transduce.reducers :as tr]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (source :as src)]
            [parkour.util :refer [ignore-errors doto-let returning]])
  (:import [clojure.lang Seqable]
           [java.io Closeable]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce
            , Job InputFormat InputSplit RecordReader]))

(defn input-format?
  [klass] (isa? klass InputFormat))

(defn rr?
  [rr] (instance? RecordReader rr))

(defn split-rr
  [^Job job ^InputFormat ifi ^InputSplit split]
  (let [tac (mr/tac job)]
    (doto (.createRecordReader ifi split tac)
      (.initialize split tac))))

(deftype RecordReaderSource
    [job mk-rr ^:unsynchronized-mutable ^RecordReader rr]
  Configurable
  (getConf [_] (conf/ig job))

  src/TupleSource
  (key [_] (.getCurrentKey rr))
  (val [_] (.getCurrentValue rr))
  (next-keyval [this]
    (if (nil? rr)
      (do (set! rr (mk-rr))
          (recur))
      (if (.nextKeyValue rr)
        this
        (src/-close this))))
  (-close [_]
    (when rr
      (.close rr)
      (set! rr nil)))
  (-nsplits [_] 1)
  (-splits [this] [this])

  java.io.Closeable
  (close [this] (src/-close this))

  ccp/CollReduce
  (coll-reduce [this f] (ccp/coll-reduce this f (f)))
  (coll-reduce [this f init] (src/source-reduce this f init))

  r/CollFold
  (coll-fold [this _ combinef reducef]
    (src/source-fold this combinef reducef))

  Seqable
  (seq [this] (src/source-seq this)))

(defn split-source
  [^Job job ^InputFormat ifi ^InputSplit split]
  (RecordReaderSource. job (partial split-rr job ifi split) nil))

(defn splits-source*
  [sources]
  (if-let [[source & sources'] (seq sources)]
    (reify
      Configurable
      (getConf [_] (conf/ig source))

      src/TupleSource
      (key [_] (src/key source))
      (val [_] (src/val source))
      (next-keyval [this]
        (let [source' (src/next-keyval source)]
          (if (identical? source source')
            this
            (if (nil? source')
              (splits-source* sources')
              (splits-source* (cons source' sources'))))))
      (-close [_] (tr/each src/-close sources))
      (-nsplits [_] (count sources))
      (-splits [this] sources)

      java.io.Closeable
      (close [this] (src/-close this))

      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [this f init] (src/source-reduce this f init))

      r/CollFold
      (coll-fold [this _ combinef reducef]
        (src/source-fold this combinef reducef))

      Seqable
      (seq [this] (src/source-seq this)))))

(defn splits-source
  [^Job job ^InputFormat ifi splits]
  (let [split-source (partial split-source job ifi)
        sources (map split-source splits)]
    (splits-source* sources)))

(defn tuple-source
  [job klass]
  (let [^InputFormat ifi (w/new-instance job klass)
        splits (seq (.getSplits ifi job))]
    (splits-source job ifi splits)))
