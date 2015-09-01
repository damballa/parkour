(ns parkour.io.mem
  (:require [parkour (conf :as conf) (reducers :as pr) (mapreduce :as mr)]
            [parkour.io (dseq :as dseq)])
  (:import [clojure.lang PersistentQueue]
           [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop RecordSeqable]))

(def ^:dynamic *max-inputs*
  "Maximum number of in-memory inputs to retain."
  11)

(def ^:private inputs
  "Collection of in-memory inputs as (name, data) pairs."
  (atom PersistentQueue/EMPTY))

(defn ^:private input-get
  "Return current input data for `iname`.  Internal."
  [iname] (->> inputs deref (pr/ffilter #(= iname (pr/nth0 %))) pr/nth1))

(defn ^:private input-add
  "Add input for `iname` with `data` to in-memory inputs."
  [iname data]
  (let [entry [iname (apply list data)]]
    (->> (fn [inputs]
           (let [inputs (conj inputs entry)]
             (if (>= *max-inputs* (count inputs))
               inputs
               (pop inputs))))
         (swap! inputs))))

(defn ^:private create-splits
  [context iname]
  [{:iname iname}])

(defn ^:private create-recseq
  {::mr/source-as :keyvals}
  [split context]
  (let [data (-> split :iname input-get)]
    (reify RecordSeqable
      (count [_] (count data))
      (seq [_] (seq data))
      (close [_]))))

(defn dseq
  "Distributed sequence producing key-value tuples from `data`, which should be
either a map or a sequence of two-item vectors, and will have the default source
shape `shape` if provided.  Stores input data in local process memory, and thus
only works for jobs run in local mode."
  ([data] (dseq :keyvals data))
  ([shape data]
   (dseq/dseq
    (fn [^Job job]
      (let [iname (-> "input__" gensym keyword)]
        (input-add iname data)
        (doto job
          (.setInputFormatClass
           (mr/input-format! job #'create-splits [iname]
                             ,,, #'create-recseq []))
          (dseq/set-default-shape! shape)))))))
