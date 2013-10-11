(ns parkour.remote.graph
  {:private true}
  (:require [parkour (conf :as conf) (reducers :as pr) (mapreduce :as mr)
                     (wrapper :as w)]
            [parkour.graph.common :as pgc])
  (:import [clojure.lang IFn$OOLL]))

(defmulti task-fn
  "Returns a function which calls `f` with an `unwrap`ed arguments and
properly `wrap`s the result."
  pr/arg0)

(defmethod task-fn :partitioner
  [_ f]
  (if (instance? IFn$OOLL f)
    (fn ^long [key val ^long nparts]
      (let [key (w/unwrap key), val (w/unwrap val)]
        (.invokePrim ^IFn$OOLL f key val nparts)))
    (fn ^long [key val ^long nparts]
      (let [key (w/unwrap key), val (w/unwrap val)]
        (f key val nparts)))))

(defmethod task-fn :default
  [_ f]
  (fn [context]
    (let [output (mr/wrap-sink context)]
      (->> context w/unwrap f (mr/sink output)))))

(defn remote-task
  [conf fkey uvar rargs jid]
  (let [uvar (do (-> uvar namespace symbol require) (resolve uvar))
        snid (conf/get-int conf "parkour.subnode" -1)
        node (pgc/job-node uvar rargs jid snid)
        fmeta (-> node fkey meta)]
    (as-> (fkey node) f
          (if-not (:hof fmeta) f (f conf))
          (if (:raw fmeta) f (task-fn fkey f)))))
