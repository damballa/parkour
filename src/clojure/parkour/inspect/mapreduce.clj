(ns parkour.inspect.mapreduce
  {:private true}
  (:require [clojure.core.protocols :as ccp]
            [parkour (mapreduce :as mr) (fs :as fs) (conf :as conf)]
            [parkour.util :refer [ignore-errors doto-let returning]])
  (:import [java.io Closeable]
           [clojure.lang Seqable]
           [org.apache.hadoop.mapreduce InputFormat RecordReader TaskAttemptID]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.util ReflectionUtils]))

(let [cfn (fn [c] (Class/forName (str "org.apache.hadoop.mapreduce." c)))
      klass (or (ignore-errors (cfn "task.TaskAttemptContextImpl"))
                (ignore-errors (cfn "TaskAttemptContext")))
      cname (-> ^Class klass .getName symbol)]
  (defmacro ^:private new-tac
    [& args] `(new ~cname ~@args)))

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

(defn records-seqable
  [conf f klass & paths]
  (let [job (doto-let [job (mr/job conf)]
              (doseq [path paths :let [path (fs/path path)]]
                (FileInputFormat/addInputPath job path)))
        conf (conf/ig job)
        ^InputFormat ifi (ReflectionUtils/newInstance klass conf)
        splits (seq (.getSplits ifi job))
        closer (agent ::initial)]
    (reify
      Closeable
      (close [_] (send closer close-rr))

      Seqable
      (seq [_]
        ((fn step [^RecordReader rr splits]
           (lazy-seq
            (if (and rr (.nextKeyValue rr))
              (cons (f [(.getCurrentKey rr) (.getCurrentValue rr)])
                    (step rr splits))
              (if (empty? splits)
                (returning nil
                  (send closer close-rr))
                (let [split (first splits), splits (rest splits),
                      tac (new-tac conf (TaskAttemptID.)),
                      rr (doto (.createRecordReader ifi split tac)
                           (.initialize split tac))]
                  (returning (step rr splits)
                    (send closer update-rr rr)))))))
         nil splits))

      ccp/CollReduce
      (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
      (coll-reduce [_ f1 init]
        (let [rra (atom ::initial)]
          (try
            (loop [acc init, splits splits, rr nil]
              (if (and rr (.nextKeyValue ^RecordReader rr))
                (let [acc (->> rr rr-tuple f (f1 acc))]
                  (if (reduced? acc)
                    @acc
                    (recur acc splits rr)))
                (if (empty? splits)
                  acc
                  (let [split (first splits), splits (rest splits)
                        tac (new-tac conf (TaskAttemptID.)),
                        rr (doto (.createRecordReader ifi split tac)
                             (.initialize split tac))]
                    (recur acc splits (swap! rra update-rr rr))))))
            (finally
              (close-rr @rra))))))))
