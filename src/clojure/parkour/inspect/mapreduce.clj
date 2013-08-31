(ns parkour.inspect.mapreduce
  {:private true}
  (:require [parkour (mapreduce :as mr) (fs :as fs)]
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

(defn records-seqable
  [conf f klass & paths]
  (let [job (doto-let [job (mr/job conf)]
              (doseq [path paths :let [path (fs/path path)]]
                (FileInputFormat/addInputPath job path)))
        conf (.getConfiguration job)
        ^InputFormat ifi (ReflectionUtils/newInstance klass conf)
        splits (seq (.getSplits ifi job))
        closer (agent ::initial)]
    (reify
      Closeable (close [_] (send closer close-rr))
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
         nil splits)))))
