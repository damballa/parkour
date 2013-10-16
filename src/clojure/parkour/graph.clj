(ns parkour.graph
  (:refer-clojure :exclude [partition shuffle])
  (:require [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
                     (reducers :as pr) (wrapper :as w)]
            [parkour.graph (tasks :as pgt) (common :as pgc) (conf :as pgconf)]
            [parkour.io.mux :as mux]
            [parkour.util :refer [ignore-errors returning var-str mpartial]])
  (:import [java.io Writer]
           [java.util Map List]
           [clojure.lang ArityException IFn APersistentMap APersistentVector]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.partition HashPartitioner]))

(comment
  ;; Example graph
  {:output [[:input] (fn [input])]}
  )

(defn ^:private graph-future
  [f inputs]
  (future
    (try
      (apply f (map deref inputs))
      (catch Throwable t
        (ignore-errors
          (->> inputs (map future-cancel) dorun))
        (throw t)))))

(defn ^:private run-parallel*
  [graph results output]
  (if-let [result (results output)]
    [results result]
    (let [[inputs f] (graph output)]
      (let [[results inputs]
            , (reduce (fn [[results inputs] input]
                        (let [[results input]
                              , (run-parallel* graph results input)]
                          [results (conj inputs input)]))
                      [results []] inputs)
            result (graph-future f inputs)
            results (assoc results output result)]
        [results result]))))

(defn run-parallel
  [graph outputs]
  (let [graph (assoc graph ::output [outputs vector])
        [_ outputs] (run-parallel* graph {} ::output)]
    (deref outputs)))

(def ^{:arglists '([job step] [step])}
  configure!
  "Update Hadoop `job` with configuration `step`, returning the mutated job."
  pgconf/configure!)

(defn ^:private mux-step
  [step] #(mux/add-subconf % (configure! (mr/job %) step)))

(defn ^:private source-config
  "Configure a job for a provided source vector."
  [^Job job source]
  (if (= 1 (count source))
    (configure! job source)
    (configure! job (mapv mux-step source))))

(declare node-config)

(defn ^:private subnode-config
  [^Job job node subnode]
  (let [node (select-keys node [:uvar :rargs :jid])
        subnode (merge node subnode)
        subconf (node-config (mr/job job) subnode)]
    (mux/add-subconf job subconf)))

(defn ^:private subnodes-config
  [^Job job node subnodes]
  (reduce (mpartial subnode-config node) job subnodes))

(defn ^:private node-config
  "Configure a job according to the state of a job graph node."
  [^Job job node]
  (doto job
    (source-config (:source node))
    (subnodes-config node (:subnodes node))
    (pgt/mapper! node)
    (pgt/combiner! node)
    (pgt/partitioner! node)
    (pgt/reducer! node)
    (configure! (:config node))
    (configure! (:sink node))))

(def ^:private stage
  "The stage of a job graph node."
  (comp :stage pr/arg0))

(defn source
  "Return a fresh `:source`-stage job graph node consuming from the
provided `dseqs`."
  [& dseqs] {:stage :source, :source (vec dseqs), :config []})

(defn config
  "Add arbitrary configuration steps to current job graph `node`."
  [node & steps] (assoc node :config (into (:config node []) steps)))

(defmulti remote
  "Create a new remote-execution task chained following the provided
job graph `node` and implemented by function `f`, which should accept
and return reducible collections of task tuples.  When producing a
`:map`-stage node, may provide a combiner function `g`, will replace
any existing job combiner function."
  {:arglists '([node f] [node f g])}
  stage)

(defmethod remote :default
  [node f]
  (let [stage (:stage node)
        msg (str "Cannot change remote-execution task from node of stage `"
                 stage "`.")]
    (throw (ex-info msg {:node node, :f f}))))

(defmethod remote :source
  ([node f] (assoc node :stage :map, :mapper f))
  ([node f g] (assoc (remote node f) :combiner g)))

(defmethod remote :map
  ([node f] (assoc node :mapper (comp f (:mapper node))))
  ([node f g] (assoc (remote node f) :combiner g)))

(defmulti partition
  "Create new reduce-partition task chained following the provided job
graph `node` and optionally implemented by function `f`, which should
follow the interface described in `parkour.mapreduce/partition!`.
Configures shuffle via `classes`, which should either be a vector of
the two map-output key & value classes, or a config step specifying
the shuffle."
  {:arglists '([node classes] [node classes f])}
  (comp type pr/arg0))

(defn shuffle
  "Base shuffle configuration; sets map output key & value types to
the classes `ckey` and `cval` respectively."
  [ckey cval]
  (fn [^Job job]
    (.setMapOutputKeyClass job ckey)
    (.setMapOutputValueClass job cval)))

(defn ^:private shuffle-classes?
  [classes]
  (and (vector? classes)
       (= 2 (count classes))
       (every? class? classes)))

(defmethod partition Map
  ([node classes]
     (partition node classes HashPartitioner))
  ([node classes f]
     (-> (assoc node :stage :partition, :partitioner f)
         (config (if-not (shuffle-classes? classes)
                   classes
                   (apply shuffle classes))))))

(defmethod partition List
  ([nodes classes]
     (partition nodes classes HashPartitioner))
  ([nodes classes f]
     (if (= 1 (count nodes))
       (partition (first nodes) classes f)
       (-> {:stage :map,
            :subnodes (vec (map-indexed #(assoc %2 :snid %1) nodes)),
            :mapper parkour.hadoop.Mux$Mapper}
           (partition classes f)))))

(defmethod remote :partition
  [node f] (assoc node :stage :reduce, :reducer f))

(defmethod remote :reduce
  [node f] (assoc node :reducer (comp f (:reducer node))))

(let [id (atom 0)]
  (defn ^:private sink-id
    "Return application-unique sink ID."
    [] (swap! id inc)))

(defmulti sink
  "Create a sink task chained following the provided job graph `node`
and sinking to the provided `dsink`."
  {:arglists '([node dsink])}
  stage)

(defmethod sink :reduce
  [node dsink] (assoc node :stage :sink, :sink dsink, :sink-id (sink-id)))

(defn ^:private map-only
  "Configuration step for map-only jobs."
  [^Job job] (.setNumReduceTasks job 0))

(defmethod sink :map
  [node dsink]
  (-> (assoc node :stage :sink, :sink dsink, :sink-id (sink-id))
      (config map-only)))

(defmethod remote :sink
  [node f] {:stage :map, :requires [node], :source [@(:sink node)], :map f})

(defn ^:private job-fn
  [conf node jname]
  (let [job (doto (mr/job conf)
              (.setJobName jname)
              (conf/set! "mapreduce.task.classpath.user.precedence" true)
              (.setJarByClass parkour.hadoop.Mappers)
              (node-config node))]
    (fn [& args]
      (try
        (returning @(:sink node)
          (when-not (.waitForCompletion job false)
            (throw (ex-info (str "Job " jname " failed.") {}))))
        (catch Throwable t
          (ignore-errors (.killJob job))
          (throw t))))))

(defn ^:private job-name
  [base n i] (format "%s[%d/%d]" base n (inc i)))

(defn execute
  ([conf uvar rargs]
     (execute conf uvar rargs []))
  ([conf uvar rargs largs]
     (let [[nodes tails] (pgc/job-graph uvar rargs largs)
           job-name (partial job-name (var-str uvar) (count nodes))
           graph (->> nodes
                      (r/map (fn [{:keys [jid requires], :as node}]
                               (let [f (job-fn conf node (job-name jid))]
                                 [jid [requires f]])))
                      (into {}))]
       (run-parallel graph tails))))
