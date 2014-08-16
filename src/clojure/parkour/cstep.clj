(ns parkour.cstep
  (:require [parkour (conf :as conf) (cser :as cser) (mapreduce :as mr)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang APersistentMap APersistentVector IFn]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapreduce Job]
           [parkour.hadoop Mappers]))

(defprotocol ConfigStep
  "Protocol for objects which add a job configuration step."
  (^:no-doc -apply! [step job] "Apply configuration `step` to Hadoop `job`."))

(defn apply!
  "Apply configuration `step` to Hadoop `job`, returning the mutated job.  The
`step` may be a function, which will be passed the `job`; a map of configuration
parameters and values; a vector of other steps; or anything implementing the
`ConfigStep` protocol."
  {:tag `Job}
  ([step] (apply! (mr/job) step))
  ([job step] (returning job (-apply! step job))))

(extend-protocol ConfigStep
  nil (-apply! [_ job] #_pass)
  Configuration (-apply! [conf job] (conf/copy! job conf))
  Job (-apply! [job' job] (conf/copy! job job'))
  APersistentMap (-apply! [m job] (conf/merge! job m))
  APersistentVector (-apply! [v job] (reduce apply! job v))
  IFn (-apply! [f job] (f job)))

(defn base*
  "Base configuration step."
  [^Job job]
  (conf/assoc! job
    "mapreduce.task.classpath.user.precedence" true
    "mapreduce.job.user.classpath.first" true
    "mapreduce.user.classpath.first" true
    "avro.serialization.data.model" "abracad.avro.ClojureData"))

(defn base
  "Base configuration step for job named `jname`."
  [jname]
  (fn [^Job job]
    (doto job
      (.setJobName jname)
      (cond-> (nil? (.getJar job)) (.setJarByClass Mappers))
      (cser/assoc! "parkour.namespaces" (loaded-libs))
      (base*))))

(defn step-map
  "Map of job configuration keys and values implemented by `step`."
  [step]
  (let [job (mr/job), job' (mr/job job)]
    (conf/diff job (apply! job' step))))
