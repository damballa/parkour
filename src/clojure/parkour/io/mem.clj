(ns parkour.io.mem
  (:require [parkour (conf :as conf) (reducers :as pr)]
            [parkour.io (dseq :as dseq)])
  (:import [clojure.lang PersistentQueue]
           [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop Mem$InputFormat]))

(def ^:dynamic *max-inputs*
  "Maximum number of in-memory inputs to retain."
  11)

(def ^:private iname-key
  "Configuration key for memory dseq input name."
  "parkour.mem.iname")

(def ^:private inputs
  "Collection of in-memory inputs as (name, data) pairs."
  (atom PersistentQueue/EMPTY))

(defn ^:internal input-get
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

(defn ^:internal get-iname
  "In-memory input name from `conf`.  Internal."
  [conf] (keyword (conf/get conf iname-key)))

(defn dseq
  "Distributed sequence producing key-value tuples from `data`, which should be
either a map or a sequence of two-item vectors.  Stores input data in local
process memory, and thus only works for jobs run in local mode."
  [data]
  (dseq/dseq
   (fn [^Job job]
     (let [iname (-> "input__" gensym keyword)]
       (input-add iname data)
       (doto job
         (conf/assoc! iname-key (name iname))
         (.setInputFormatClass Mem$InputFormat))))))
