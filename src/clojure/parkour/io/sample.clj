(ns parkour.io.sample
  (:require [parkour (conf :as conf) (cstep :as cstep) (wrapper :as w)
             ,       (mapreduce :as mr) (reducers :as pr)]
            [parkour.io (dseq :as dseq)])
  (:import [java.util Random]
           [org.apache.hadoop.mapreduce InputFormat Job]))

(defn ^:private create-splits
  [context klass nsplits seed]
  (let [inform ^InputFormat (w/new-instance context klass)
        splits (.getSplits inform context)
        rnd (Random. seed)]
    (pr/sample-reservoir rnd nsplits splits)))

(defn ^:private create-rr
  {::mr/adapter identity}
  [split context klass]
  (let [inform ^InputFormat (w/new-instance context klass)]
    (.createRecordReader inform split context)))

(def ^:private defaults
  "Default values for sample dseq options."
  {:size 131072,
   :splits 5,
   :seed 1,
   })

(defn dseq
  "Distributed sequence which samples from distributed sequence `step`, as
optionally configured by the map `options`.  Available options are:
  `:size` -- Sample split size (default 131072);
  `:splits` -- Number of splits to sample (default 5);
  `:seed` -- Seed for random sampling process (default 1)."
  ([step] (dseq {} step))
  ([options step]
   (let [{:keys [splits size seed]} (merge defaults options)]
     (dseq/dseq
      (fn [^Job job]
        (cstep/apply! job step)
        (let [klass (.getInputFormatClass job)]
          (doto job
            (conf/assoc! "mapred.max.split.size" size)
            (.setInputFormatClass
             (mr/input-format! job #'create-splits [klass splits seed]
                               ,,, #'create-rr [klass])))))))))
