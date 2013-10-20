(ns parkour.remote.graph
  {:private true}
  (:require [parkour (reducers :as pr) (mapreduce :as mr) (wrapper :as w)]
            [parkour.graph.common :as pgc]
            [parkour.util :refer [mcomp ffilter]])
  (:import [clojure.lang IFn$OOLL]))

(defn job-node
  "Given provided graph-generation user var `uvar`, remote arguments
`rargs`, job ID `jid`, and subnode ID `snid`, return job matching
graph node."
  [uvar rargs jid snid]
  (-> (pgc/job-graph uvar rargs []) first (get jid)
      (cond-> snid (as-> node (merge node (-> node :subnodes (get snid)))))))

(defn raw? [f] (-> f meta :raw))
(defn !hof? [f] (-> f meta :hof not))
(defn ctxt? [f] (-> f meta :context))

(def task-fn nil)
(defmulti task-fn
  "Wrap function/-list `f` as specified in metadata, for purpose `fkey`, using
configuration `conf`."
  {:arglists '([conf fkey f])}
  pr/arg1)

(defn task-partitioner
  [f]
  (if (instance? IFn$OOLL f)
    (fn ^long [key val ^long nparts]
      (let [key (w/unwrap key), val (w/unwrap val)]
        (.invokePrim ^IFn$OOLL f key val nparts)))
    (fn ^long [key val ^long nparts]
      (let [key (w/unwrap key), val (w/unwrap val)]
        (f key val nparts)))))

(defmethod task-fn :partitioner
  [conf fkey f]
  (as-> f f' (if (!hof? f) f' (f' conf))
        ,,,, (if (raw? f)  f' (task-partitioner f))))

(defn task-remote
  [f]
  (fn [context]
    (->> context w/unwrap (f context) (mr/sink context))))

(defmethod task-fn :default
  [conf fkey fs]
  (or (ffilter raw? fs)
      (->> fs
           (map (fn [f]
                  (as-> f f' (if (!hof? f) f' (f' conf))
                        ,,,, (if (ctxt? f) f' (comp f' pr/arg1)))))
           (apply mcomp)
           (task-remote))))

(defn remote-task
  "Remote task entry point for `parkour.graph` API tasks."
  [conf fkey uvar rargs jid snid]
  (let [uvar (do (-> uvar namespace symbol require) (resolve uvar))
        node (job-node uvar rargs jid snid)]
    (task-fn conf fkey (fkey node))))
