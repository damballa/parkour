(ns parkour.graph
  (:refer-clojure :exclude [partition shuffle])
  (:require [clojure.core :as cc]
            [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
                     (reducers :as pr) (wrapper :as w)]
            [parkour.graph (tasks :as pgt) (common :as pgc) (cstep :as cstep)
                           (dseq :as dseq) (dsink :as dsink)]
            [parkour.io (mux :as mux) (dux :as dux)]
            [parkour.util
             :refer [ignore-errors returning var-str mpartial mcomp]])
  (:import [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.partition HashPartitioner]))

(defn ^:private graph-future
  "Return future result of applying function `f` to the values held by futures
`inputs`.  Attempts to cancel all inputs upon any failures."
  [f inputs]
  (future
    (try
      (apply f (map deref inputs))
      (catch Throwable t
        (ignore-errors
          (->> inputs (map future-cancel) dorun))
        (throw t)))))

(defn ^:private run-parallel*
  "Build a graph of futures for the data-flow described by the map `graph`.
The `results` map holds calculated result futures, and `output` is the key of
the desired result.  Returns a tuple of updated `(results, result)`."
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
  "Execute in parallel the data-flow graph described by `graph`.  Each key in
`graph` identifies a particular entry.  Each value is a tuple of `(inputs, f)`,
where `inputs` is a sequence of other `graph` keys and `f` is a function
calculating that entry's result given `inputs`.  Returns a vector of the result
entries for the keys in the collection `outputs`."
  [graph outputs]
  (let [graph (assoc graph ::output [outputs vector])
        [_ outputs] (run-parallel* graph {} ::output)]
    (deref outputs)))

(declare node-config)

(defn ^:private subnode-config
  "Configure `job` for `subnode` of `node`."
  [^Job job node subnode]
  (let [node (select-keys node [:uvar :rargs :jid])
        subnode (merge node subnode)
        subconf (node-config (mr/job job) subnode)]
    (mux/add-subconf job subconf)))

(defn ^:private subnodes-config
  "Configure `job` for all `subnodes` of `node`."
  [^Job job node subnodes]
  (reduce (mpartial subnode-config node) job subnodes))

(defn ^:private node-config
  "Configure `job` according to the state of a job graph node `node`."
  [^Job job node]
  (doto job
    (cstep/apply! (:source node))
    (subnodes-config node (:subnodes node))
    (pgt/mapper! node)
    (pgt/combiner! node)
    (pgt/partitioner! node)
    (pgt/reducer! node)
    (cstep/apply! (:config node))
    (cstep/apply! (:sink node))))

(def ^:private stage
  "The job stage of job graph node `node`."
  (fn [node & args]
    (cond
     (vector? node) ::vector
     (map? node) (:stage node)
     :else (throw (ex-info "Invalid `node`" {:node node})))))

(defn ^:private apply-meta
  "Parse `coll` as sequences of keyword-value pairs interspersed with individual
values.  Apply keyword-value pairs as metadata to their subsequent individual
values.  Return sequence of values."
  [coll]
  (first
   (reduce (fn [[values md kw] x]
             (cond
              kw
              , [values (assoc md kw x) nil]
              (keyword? x)
              , [values md x]
              :else
              , [(conj values (vary-meta x merge md)) {} nil]))
           [[] {} nil] coll)))

(let [id (atom 0)]
  (defn ^:private gen-id
    "Return application-unique source/sink ID."
    [] (swap! id inc)))

(defn ^:private f-list
  "If `f` is a class, return it.  Else return list containing `f`."
  [f] (if (class? f) f (list f)))

(defn source
  "Return a fresh `:source`-stage job graph node consuming from the
provided `dseq`."
  [dseq]
  {:stage :source,
   :source-id (gen-id),
   :source (dseq/dseq dseq),
   :config [],
   })

(defn config
  "Add arbitrary configuration steps to job graph node `node`."
  [node & steps] (assoc node :config (into (:config node []) steps)))

(def ^:private remote* nil)
(defmulti ^:private remote*
  "Internal dispatch multimethod for `remote` implementations."
  {:arglists '([node f] [node f g])}
  stage)

(defn remote
  "Create a new remote-execution task chained following the provided job graph
`node` and implemented by function `f`.  When producing a `:map`-stage node, may
provide a combiner function `g`, which will replace any existing job combiner
function.  Functions may be preceded any number of `option` keyword-value pairs,
which will be applied as metadata to the following function."
  {:arglists '([node option* f] [node option* f option* g])}
  [node & args] (apply remote* node (apply-meta args)))

(defmethod remote* :default
  [node & fs]
  (let [msg (str "Cannot chain `remote` from stage `" (stage node) "`.")]
    (throw (ex-info msg {:node node}))))

(defmethod remote* ::vector
  [nodes & fs]
  (if-not (every? (comp #{:source} stage) nodes)
    (throw (ex-info "Cannot merge non-`:source` nodes." {:nodes nodes}))
    (let [node (assoc (source (apply mux/dseq (map :source nodes)))
                 :requires (into [] (mapcat :requires nodes)))]
      (apply remote node fs))))

(defmethod remote* :source
  ([node f] (assoc node :stage :map, :mapper (f-list f)))
  ([node f g] (assoc (remote node f) :combiner (f-list g))))

(defmethod remote* :map
  ([node f] (assoc node :mapper (cons f (:mapper node))))
  ([node f g] (assoc (remote node f) :combiner (f-list g))))

(def ^:private partition* nil)
(defmulti ^:private partition*
  "Internal dispatch multimethod for `partition` implementations."
  {:arglists '([node classes f])}
  stage)

(defn partition
  "Create new partition task chained following the provided job graph `node`, as
configured by `step` and optionally implemented by `f`.  The `node` may be
either a single job graph node or a vector of job graph nodes to co-group.  The
`step` may be either a configuration step or a vector of the two map-output key
& value classes.  The `f` partitioner may be a function or a `Partitioner`
class.  If a function `f` is provided, it may be preceded by any number of
`option` keyword-value pairs, which will be applied to it as metadata."
  {:arglists '([node step] [node step option* f])}
  ([node step] (partition* node step HashPartitioner))
  ([node step & args] (apply partition* node step (apply-meta args))))

(defn shuffle
  "Base shuffle configuration; sets map output key & value types to
the classes `ckey` and `cval` respectively."
  [ckey cval]
  (fn [^Job job]
    (.setMapOutputKeyClass job ckey)
    (.setMapOutputValueClass job cval)))

(defn ^:private shuffle-classes?
  "True iff `classes` is a vector of two classes."
  [classes]
  (and (vector? classes)
       (= 2 (count classes))
       (every? class? classes)))

(defmethod partition* ::vector
  [nodes classes f]
  (if (= 1 (count nodes))
    (partition (first nodes) classes f)
    (-> {:stage :map,
         :subnodes (vec (map-indexed #(assoc %2 :snid %1) nodes)),
         :mapper parkour.hadoop.Mux$Mapper}
        (partition classes f))))

(defmethod partition* :map
  [node classes f]
  (-> (assoc node :stage :partition, :partitioner f)
      (config (if-not (shuffle-classes? classes)
                classes
                (apply shuffle classes)))))

(defmethod remote* :partition
  [node f] (assoc node :stage :reduce, :reducer (f-list f)))

(defmethod remote* :reduce
  [node f] (assoc node :reducer (cons f (:reducer node))))

(defn ^:private sink*
  "Chain sink task following job graph node `node`."
  [node dsink] (assoc node :stage :sink, :sink dsink, :sink-id (gen-id)))

(def sink nil)
(defmulti sink
  "Create a sink task chained following the job graph node `node` and sinking to
`dsink`.  Yields a new `:source`-stage node reading from the sunk output."
  {:arglists '([node dsink])}
  stage)

(defn ^:private map-only
  "Configuration step for map-only jobs."
  [^Job job] (.setNumReduceTasks job 0))

(defmethod sink :map
  [node dsink]
  (-> node (config map-only) (assoc :stage reduce) (sink dsink)))

(defmethod sink :reduce
  [node dsink]
  (let [dsink (dsink/dsink dsink), dseq (dseq/dseq dsink),
        node (sink* node dsink)]
    (assoc (source dseq) :requires [node])))

(def sink-multi nil)
(defmulti sink-multi
  "Create a sink task chained following the job graph node `node` and sinking to
the provided `named-dsinks`, which should consist of alternating name/dsink
pairs.  Yields a vector of new `:source`-stage nodes reading from each of the
sunk outputs."
  {:arglists '([node & named-dsinks])}
  stage)

(defmethod sink-multi :map
  [node & named-dsinks]
  (apply sink-multi (-> node (config map-only) (assoc :stage reduce))
         ,          named-dsinks))

(defmethod sink-multi :reduce
  [node & named-dsinks]
  (let [dsink (apply dux/dsink named-dsinks)
        dseqs (map (comp dseq/dseq second) (cc/partition 2 named-dsinks))
        node (sink* node dsink)]
    (mapv #(assoc (source %) :requires [node]) dseqs)))

(def ^:private job-fn nil)
(defmulti ^:private job-fn
  "Return job-execution function for provided `node`, with base Hadoop
configuration `conf` and job name `jname`"
  {:arglists '([node conf jname])}
  stage)

(defmethod job-fn :source
  [node conf jname]
  (constantly (:source node)))

(defmethod job-fn :default
  [node conf jname]
  (fn [& args]
    (let [job (doto (mr/job conf)
                (cstep/apply! (cstep/base jname))
                (node-config node))]
      (try
        (returning true
          (log/info "Launching job" jname)
          (when-not (.waitForCompletion job false)
            (throw (ex-info (str "Job " jname " failed.")
                            {:jname jname, :job job, :node node})))
          (log/info "Job" jname "completed successfully"))
        (catch Throwable t
          (ignore-errors (.killJob job))
          (throw t))))))

(defn ^:private job-name
  "Job name for `i`th job of `n` produced from var-name `base`."
  [base n i] (format "%s[%d/%d]" base (inc i) n))

(defn execute
  "Execute Hadoop jobs for the job graph produced by invoking the var `uvar`.
Invokes `uvar` with the `rargs` vector of remote & local arguments and the
`lvar` vector of local-only arguments (or the empty vector, if not provided).
Jobs are configured starting with base configuration `conf`.  Returns a vector
of the distributed sequences produced by the job graph leaves."
  ([conf uvar rargs]
     (execute conf uvar rargs []))
  ([conf uvar rargs largs]
     (let [[nodes tails] (pgc/job-graph uvar rargs largs)
           job-name (partial job-name (var-str uvar) (count nodes))
           graph (->> nodes
                      (r/map (fn [{:keys [jid requires], :as node}]
                               (let [f (job-fn node conf (job-name jid))]
                                 [jid [requires f]])))
                      (into {}))]
       (run-parallel graph tails))))
