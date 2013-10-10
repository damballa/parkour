(ns parkour.graph.tasks
  {:private true}
  (:require [parkour (conf :as conf) (reducers :as pr) (mapreduce :as mr)
                     (wrapper :as w)]
            [parkour.util :refer [var-symbol]])
  (:import [clojure.lang IFn$OOLL]
           [org.apache.hadoop.mapreduce Job Mapper Partitioner Reducer]))

(defn ^:private job-graph*
  [uvar rargs largs]
  (let [graph (uvar rargs largs)
        tails (if (vector? graph) graph [graph])]
    (->> (iterate (partial mapcat :requires) tails)
         (take-while seq)
         (apply concat)
         (reduce (fn [[_ jids jobs :as state] {:keys [sink], :as node}]
                   (if-let [jid (jids sink)]
                     state
                     (let [jid (count jids)
                           node (assoc node :jid jid, :uvar uvar, :rargs rargs)]
                       [tails (assoc jids sink jid) (conj jobs node)])))
                 [tails {} []]))))

(defn job-graph
  [uvar rargs largs]
  (let [[tails jids jobs] (job-graph* uvar rargs largs)
        rjid (partial - (-> jids count dec))
        rjids (comp rjid jids)]
    [(mapv (fn [{:keys [jid requires], :or {requires []}, :as node}]
             (let [jid (rjid jid), requires (mapv (comp rjids :sink) requires)]
               (assoc node :jid jid :requires requires)))
           (rseq jobs))
     (mapv (comp rjids :sink) tails)]))

(defn job-node
  [uvar rargs jid]
  (-> (job-graph uvar rargs []) first (get jid)))

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
        sni (conf/get-int conf "parkour.subnode" -1)
        node (cond-> (job-node uvar rargs jid)
                     (not (neg? sni))
                     , (as-> node (merge node (-> node :subnodes (get sni)))))
        fmeta (-> node fkey meta)]
    (as-> (fkey node) f
          (if-not (:hof fmeta) f (f conf))
          (if (:raw fmeta) f (task-fn fkey f)))))

(defn task!
  [job node fkey klass alloc]
  (let [impl (fkey node)]
    (if (isa? impl klass)
      impl
      (let [{:keys [uvar rargs jid]} node]
        (alloc job #'remote-task fkey (var-symbol uvar) rargs jid)))))

(defmacro deftask
  [fname fkey alloc klass method]
  `(defn ~fname
     [~(with-meta 'job {:tag `Job}) ~'node]
     (when (~fkey ~'node)
       (. ~'job ~method (task! ~'job ~'node ~fkey ~klass ~alloc)))))

(deftask mapper! :mapper mr/mapper! Mapper setMapperClass)
(deftask combiner! :combiner mr/combiner! Reducer setCombinerClass)
(deftask partitioner!
  :partitioner mr/partitioner! Partitioner setPartitionerClass)
(deftask reducer! :reducer mr/reducer! Reducer setReducerClass)
