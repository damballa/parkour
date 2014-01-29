(ns parkour.io.sample
  (:require [parkour (conf :as conf) (cstep :as cstep)]
            [parkour.io (dseq :as dseq)])
  (:import [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop Sample$InputFormat]))

(defn dseq
  "Distributed sequence which samples from distributed sequence `step`, as
configured by keyword argument `options`.  Available options are:
  `:size` -- Sample split size (default 65536);
  `:n` -- Number of splits to sample (default 5);
  `:seed` -- Seed for random sampling process (default 1)."
  [step & options]
  (let [{:keys [n size seed] :or {n 5, size 65536, seed 1}} options]
    (dseq/dseq
     (fn [^Job job]
       (doto job
         (cstep/apply! step)
         (conf/assoc! #_job
           "mapred.max.split.size" size
           "parkour.sample.class" (.getInputFormatClass job)
           "parkour.sample.n" n
           "parkour.sample.seed" seed)
         (.setInputFormatClass Sample$InputFormat))))))
