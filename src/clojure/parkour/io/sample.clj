(ns parkour.io.sample
  (:require [parkour (conf :as conf) (cstep :as cstep)]
            [parkour.io (dseq :as dseq)])
  (:import [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop Sample$InputFormat]))

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
          (doto job
            (cstep/apply! step)
            (conf/assoc! #_job
              "mapred.max.split.size" size
              "parkour.sample.class" (.getInputFormatClass job)
              "parkour.sample.splits" splits
              "parkour.sample.seed" seed)
            (.setInputFormatClass Sample$InputFormat)))))))
