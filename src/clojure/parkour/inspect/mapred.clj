(ns parkour.inspect.mapred
  (:require [parkour (fs :as fs)]
            [parkour.util :refer [doto-let returning]])
  (:import [java.io Closeable]
           [clojure.lang Seqable]
           [org.apache.hadoop.mapred
             FileInputFormat InputFormat JobConf RecordReader Reporter]
           [org.apache.hadoop.util ReflectionUtils]))

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

(defn records-seqable
  [conf klass f & paths]
  (let [conf (doto-let [conf (JobConf. conf)]
               (doseq [path paths :let [path (fs/path path)]]
                 (FileInputFormat/addInputPath conf path)))
        ^InputFormat ifi (ReflectionUtils/newInstance klass conf)
        splits (seq (.getSplits ifi conf 1))
        closer (agent ::initial)]
    (reify
      Closeable (close [_] (send closer close-rr))
      Seqable
      (seq [_]
        ((fn step [^RecordReader rr key val splits]
           (lazy-seq
            (if (and rr (.next rr key val))
              (cons (f [key val]) (step rr key val splits))
              (if (empty? splits)
                (returning nil
                  (send closer close-rr))
                (let [split (first splits), splits (rest splits),
                      rr (.getRecordReader ifi split conf Reporter/NULL)
                      key (.createKey rr), val (.createValue rr)]
                  (returning (step rr key val splits)
                    (send closer update-rr rr)))))))
         nil nil nil splits)))))
