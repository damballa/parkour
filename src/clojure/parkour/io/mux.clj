(ns parkour.io.mux
  (:require [clojure.edn :as edn]
            [parkour (conf :as conf) (cstep :as cstep) (mapreduce :as mr)]
            [parkour.io (dseq :as dseq)]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop Mux$InputFormat]))

(def ^:private ^:const confs-key
  "parkour.mux.confs")

(defn ^:private mux-input?
  "True iff `job` is configured for multiplex input."
  [job] (->> (conf/get-class job "mapreduce.inputformat.class" nil)
             (identical? Mux$InputFormat)))

(defn ^:private mux-empty
  "Clone of `job` with empty multiplex sub-configurations vector."
  [job] (-> job mr/job (conf/assoc! confs-key "[]")))

(defn get-subconfs
  "Get sequence of `job` multiplex sub-configuration diffs."
  [job]
  (or (if (mux-input? job)
        (some->> (conf/get job confs-key) (edn/read-string)))
      []))

(defn add-subconf
  "Add multiplex input format sub-configuration."
  [^Job job subconf]
  (if-not (mux-input? subconf)
    (let [diff (-> job (conf/diff subconf) (dissoc confs-key)),
          diffs (get-subconfs job)]
      (doto job
        (.setInputFormatClass Mux$InputFormat)
        (conf/assoc! confs-key (-> diffs (conj diff) pr-str))))
    (reduce #(add-subconf %1 (-> subconf mr/job (conf/merge! %2)))
            job (get-subconfs subconf))))

(defn add-substep
  "Add configuration changes produced by `step` as a multiplex
sub-configuration of `job`."
  [^Job job step]
  (add-subconf job (-> job mux-empty (cstep/apply! step))))

(defn dseq
  "Multiplex distributed sequence, consisting of any number of other
job configuration `steps`."
  [& steps]
  (dseq/dseq
   (fn [^Job job]
     (reduce add-substep job steps))))
