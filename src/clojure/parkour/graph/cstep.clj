(ns parkour.graph.cstep
  (:require [parkour (conf :as conf) (mapreduce :as mr)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang APersistentMap APersistentVector IFn]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapreduce Job]))

(defprotocol ConfigStep
  "Protocol for objects which add a job configuration step."
  (-apply! [step job] "Apply configuration `step` to Hadoop `job`."))

(defn apply!
  "Apply configuration `step` to Hadoop `job`, returning the mutated job.  The
`step` may be a function, which will be passed the `job`; a map of configuration
parameters and values; a vector of other steps; or anything implementing the
`ConfigStep` protocol."
  ([step] (apply! (mr/job) step))
  ([job step] (returning job (-apply! step job))))

(extend-protocol ConfigStep
  nil (-apply! [_ job] #_pass)
  Configuration (-apply! [conf job] (conf/copy! job conf))
  Job (-apply! [job' job] (conf/copy! job job'))
  APersistentMap (-apply! [m job] (conf/merge! job m))
  APersistentVector (-apply! [v job] (reduce apply! job v))
  IFn (-apply! [f job] (f job)))

(defn step-map
  "Map of job configuration keys and values implemented by `step`."
  [step]
  (let [job (mr/job), job' (mr/job job)]
    (conf/diff job (apply! job' step))))
