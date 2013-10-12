(ns parkour.graph.conf
  (:require [parkour (conf :as conf) (mapreduce :as mr)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang APersistentMap APersistentVector IFn]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapreduce Job]))

(defprotocol ConfigStep
  "Protocol for objects which add a job configuration step."
  (-configure [this job] "Mutate configuration of Hadoop Job `job`."))

(defn configure!
  "Update Hadoop `job` with configuration `step`, returning the mutated job."
  ([job step] (returning job (-configure step job)))
  ([step] (configure! (mr/job) step)))

(extend-protocol ConfigStep
  nil (-configure [_ job] #_pass)
  Configuration (-configure [conf job] (conf/copy! job conf))
  Job (-configure [job' job] (conf/copy! job job'))
  APersistentMap (-configure [m job] (conf/merge! job m))
  APersistentVector (-configure [v job] (reduce configure! job v))
  IFn (-configure [f job] (f job)))

(defn step-map
  "Map of job configuration keys and values implemented by `step`."
  [step]
  (let [job (mr/job), job' (mr/job job)]
    (conf/diff job (configure! job' step))))
