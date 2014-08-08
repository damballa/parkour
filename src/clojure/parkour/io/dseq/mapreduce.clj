(ns parkour.io.dseq.mapreduce
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [transduce.reducers :as tr]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (source :as src)]
            [parkour.util :refer [ignore-errors returning realized-seq]])
  (:import [clojure.lang IPending Seqable]
           [java.io Closeable]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce
            , Job InputFormat InputSplit RecordReader]))

(defn input-format?
  [klass] (isa? klass InputFormat))

(defn rr?
  [rr] (instance? RecordReader rr))

(defn split-source
  [^Job job ^InputFormat ifi ^InputSplit split]
  (let [tac (mr/tac job), rr (.createRecordReader ifi split tac)]
    (reify
      Configurable
      (getConf [_] (conf/ig job))

      src/TupleSource
      (key [_] (.getCurrentKey rr))
      (val [_] (.getCurrentValue rr))
      (next-keyval [this] (.nextKeyValue rr))
      (-initialize [_] (.initialize rr split tac))
      (-close [this] (.close this))
      (-nsplits [_] 1)
      (-splits [this] [this])

      Closeable
      (close [this] (ignore-errors (.close rr)))

      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [this f init] (src/source-reduce this f init))

      r/CollFold
      (coll-fold [this _ combinef reducef]
        (src/source-fold this combinef reducef))

      Seqable
      (seq [this] (src/source-seq this)))))

(defn splits-source*
  [job sources]
  (reify
    Configurable
    (getConf [_] (conf/ig job))

    src/TupleSource
    (-close [_] (tr/each src/-close (realized-seq sources)))
    (-nsplits [_] (count sources))
    (-splits [this] sources)

    Closeable
    (close [this] (src/-close this))

    ccp/CollReduce
    (coll-reduce [this f] (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init] (src/source-reduce this f init))

    r/CollFold
    (coll-fold [this _ combinef reducef]
      (src/source-fold this combinef reducef))

    Seqable
    (seq [this] (src/source-seq this))))

(defn splits-source
  [^Job job ^InputFormat ifi splits]
  (let [split-source (partial split-source job ifi)
        sources (map split-source splits)]
    (splits-source* job sources)))

(defn tuple-source
  [job klass]
  (let [^InputFormat ifi (w/new-instance job klass)
        splits (seq (.getSplits ifi job))]
    (case (count splits)
      0 nil
      1 (split-source job ifi (first splits))
      , (splits-source job ifi splits))))
