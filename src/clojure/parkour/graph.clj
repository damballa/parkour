(ns parkour.graph
  (:refer-clojure :exclude [map partition shuffle reduce])
  (:require [clojure.core :as cc]
            [clojure.core.protocols :as ccp]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :as log]
            [parkour (conf :as conf) (fs :as fs) (cstep :as cstep)
                     (wrapper :as w) (mapreduce :as mr) (reducers :as pr)]
            [parkour.io (dseq :as dseq) (dsink :as dsink)
                        (mux :as mux) (dux :as dux)]
            [parkour.util :refer [ignore-errors returning doto-let mpartial]])
  (:import [java.util.concurrent ExecutionException]
           [clojure.lang Var]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.partition HashPartitioner]))

(defn ^:private graph-future
  "Return future result of applying function `f` to the values held by futures
`inputs`.  Attempts to cancel all inputs upon any failures."
  [f inputs]
  (future
    (try
      (apply f (cc/map deref inputs))
      (catch Throwable t
        (ignore-errors
          (->> inputs (cc/map future-cancel) dorun))
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
            , (cc/reduce (fn [[results inputs] input]
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
    (try
      (deref outputs)
      (catch ExecutionException e
        (throw
         (loop [^Throwable e e]
           (let [e' (.getCause e)]
             (cond
              (nil? e') e
              (not (instance? ExecutionException e')) e'
              :else (recur e'))))))
      (catch Exception e
        (ignore-errors (future-cancel outputs))
        (throw e)))))

(defn ^:private stage
  "The job stage of job node `node`."
  [node & args]
  (cond
   (vector? node) ::vector
   (map? node) (:stage node)
   :else (throw (ex-info "Invalid `node`" {:node node}))))

(defn ^:private error
  [verb node]
  (let [msg (str "Cannot `" verb "` from stage `" (stage node) "`.")]
    (throw (ex-info msg {:node node}))))

(let [id (atom 0)]
  (defn ^:private gen-id
    "Return application-unique input/output ID."
    [] (swap! id inc)))

(defn input
  "Return a fresh `:input`-stage job graph node consuming from the provided
`dseq`.  If instead provided a `:input`-stage node, will return it."
  [dseq]
  (if (identical? :input (:stage dseq))
    dseq
    {:stage :input,
     :input-id (gen-id),
     :config [(dseq/dseq dseq)],
     :requires [],
     }))

(def
  ^{:deprecated true, :arglists '([dseq])}
  source
  "Deprecated alias for `input`."
  input)

(defn config
  "Add arbitrary configuration steps to `node`, which may be either a single job
node or a vector of job nodes."
  [node & steps]
  (if (identical? ::vector (stage node))
    (mapv #(apply config % steps) node)
    (assoc node :config (-> node :config (into steps)))))

(def ^:private remote-config nil)
(defmulti ^:private remote-config
  "Return config step for allocating remote task classes."
  {:arglists '([alloc set-class cls] [alloc set-class uvar & args])}
  (fn [alloc set-class cls-var & args] (type cls-var)))

(defmethod remote-config Class
  [alloc set-class cls] (mpartial set-class cls))

(defmethod remote-config Var
  [alloc set-class uvar & args]
  (fn [job] (set-class job (apply alloc job uvar args))))

(defmethod remote-config :default
  [_ _ task & args]
  (let [msg (str "Invalid task implementation `" task `";"
                 " tasks may only be implemented by classes or vars.")]
   (throw (ex-info msg {:task task}))))

(defmacro ^:private defremotes
  [& args]
  `(do ~@(cc/map (fn [[name alloc set-class]]
                   `(def ~(vary-meta name assoc :private true)
                      (partial remote-config ~alloc ~set-class)))
                 (cc/partition 3 args))))

(defremotes
  mapper-config mr/mapper! mr/set-mapper
  combiner-config mr/combiner! mr/set-combiner
  reducer-config mr/reducer! mr/set-reducer
  partitioner-config mr/partitioner! mr/set-partitioner)

(def map nil)
(defmulti map
  "Add map task to job node `node`, as implemented by `Mapper` class `cls`, or
Clojure var `var` and optional `args`."
  {:arglists '([node cls] [node var & args])}
  stage)

(defmethod map :default
  [node & more] (error "map" node))

(defmethod map ::vector
  [nodes & more]
  (if-not (every? (comp #{:input} stage) nodes)
    (throw (ex-info "Cannot merge non-`:input` nodes." {:nodes nodes}))
    (let [node (assoc (input (apply mux/dseq (cc/map :config nodes)))
                 :requires (into [] (mapcat :requires nodes)))]
      (apply map node more))))

(defmethod map :input
  [node mapper & args]
  (let [step (apply mapper-config mapper args)]
    (-> node (config step) (assoc :stage :map))))

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

(def ^:private partition* nil)
(defmulti ^:private partition*
  "Internal dispatch multimethod for `partition` implementations."
  {:arglists '([node step] [node step cls] [node step var & args])}
  stage)

(defn partition
  "Add partition task to the provided job node `node`, as configured by `step`
and optionally implemented by either Partitioner class `cls` or Clojure var
`var` & optional `args`.  The `node` may be either a single job node or a vector
of job nodes to co-group.  The `step` may be either a configuration step or a
vector of the two map-output key & value classes."
  {:arglists '([node step] [node step cls] [node step var & args])}
  ([node step] (partition* node step HashPartitioner))
  ([node step & args] (apply partition* node step args)))

(defmethod partition* :default
  [node & more] (error "partition" node))

(defmethod partition* ::vector
  [nodes classes cls-var & args]
  (if (= 1 (count nodes))
    (apply partition (first nodes) classes cls-var args)
    (let [steps (mapv #(mpartial mux/add-substep (:config %)) nodes)
          mapper (mapper-config parkour.hadoop.Mux$Mapper)
          node {:stage :map,
                :input-id (gen-id),
                :config (conj steps mapper),
                :requires (into [] (mapcat :requires nodes))}]
      (apply partition node classes cls-var args))))

(defmethod partition* :map
  [node classes cls-var & args]
  (-> (assoc node :stage :partition)
      (config (apply partitioner-config cls-var args)
              (if-not (shuffle-classes? classes)
                classes
                (apply shuffle classes)))))

(defn combine
  "Add combine task to job node `node`, as implemented by `Reducer` class `cls`,
or Clojure var `var` and optional `args`."
  {:arglists '([node cls] [node var & args])}
  [node cls-var & args]
  (if-not (identical? :partition (stage node))
    (error "combine" node)
    (let [step (apply combiner-config cls-var args)]
      (-> node (config step) (assoc :stage :combine)))))

(defn reduce
  "Add reduce task to job node `node`, as implemented by `Reducer` class `cls`,
or Clojure var `var` and optional `args`."
  {:arglists '([node cls] [node var & args])}
  [node cls-var & args]
  (if-not (#{:partition :combine} (stage node))
    (error "reduce" node)
    (let [step (apply reducer-config cls-var args)]
      (-> node (config step) (assoc :stage :reduce)))))

(defn ^:private re-input
  [node dsink] (-> dsink dsink/dsink-dseq input (assoc :requires [node])))

(defn ^:private map-only
  "Configuration step for map-only jobs."
  [^Job job] (.setNumReduceTasks job 0))

(defn ^:private output*
  "Chain output task following job node `node`."
  [node dsink]
  (if-not (map? node)
    (error "output" node)
    (-> (assoc node :stage :output, :output-id (gen-id))
        (cond-> (identical? :map (stage node)) (config map-only))
        (config dsink))))

(defn output
  "Add output task to job node `node` for writing to `dsink` or named-output
name-dsink pairs `named-dsinks`.  Yields either a new `:input`-stage node
reading from the written output or a vector of such nodes."
  {:arglists '([node dsink] [node & named-dsinks])}
  ([node dsink]
     (-> node (output* dsink) (re-input dsink)))
  ([node dsinks & rest]
     (let [named-dsinks (cons dsinks rest),
           dsinks (take-nth 2 (drop 1 named-dsinks)),
           node (->> named-dsinks (apply hash-map) dux/dsink (output* node))]
       (mapv (partial re-input node) dsinks))))

(def
  ^{:deprecated true, :arglists '([node dsink] [node & named-dsinks])}
  sink
  "Deprecated alias for `output`."
  output)

(defn node-job
  "Hadoop `Job` for job node `node`, starting with base configuration `conf`
and named `jname`."
  {:tag `Job}
  [node conf jname]
  (doto (mr/job conf)
    (cstep/apply! (cstep/base jname))
    (cstep/apply! (:config node))))

(defmacro ^:private with-shutdown-hook
  "Execute `body` with function `f` as a registered shutdown hook for `body`'s
dynamic scope."
  [f & body]
  `(let [hook# (Thread. ~f)]
    (try
      (-> (Runtime/getRuntime) (.addShutdownHook hook#))
      ~@body
      (finally
        (-> (Runtime/getRuntime) (.removeShutdownHook hook#))))))

(defn run-job
  "Run `job` and wait synchronously for it to complete.  Kills the job on
exceptions or JVM shutdown.  Unlike the `Job#waitForCompletion()` method, does
not swallow `InterruptedException`."
  [^Job job]
  (let [interval (conf/get-int job "jobclient.completion.poll.interval" 5000)
        jname (.getJobName job)
        abort (fn []
                (log/warn "Stopping job" jname)
                (ignore-errors (.killJob job)))]
    (with-shutdown-hook abort
      (try
        (log/info "Launching job" jname)
        (.submit job)
        (while (not (.isComplete job))
          (Thread/sleep interval))
        (doto-let [result (.isSuccessful job)]
          (if result
            (log/info "Job" jname "succeeded")
            (log/warn "Job" jname "failed")))
        (catch Exception e
          (abort)
          (throw e))))))

(def node-fn nil)
(defmulti node-fn
  "Return a function for executing the job defined by the job node `node`, using
base configuration `conf` and job name `jname`."
  {:arglists '([node conf jname])}
  stage)

(defmethod node-fn :input
  [node conf jname]
  (fn [^Job job]
    (-> node :config first (vary-meta assoc ::mr/counters (.getCounters job)))))

(defmethod node-fn :default
  [node conf jname]
  (fn [& args]
    (doto-let [job (node-job node conf jname)]
      (when-not (run-job job)
        (throw (ex-info (str "Job " jname " failed.") {:jname jname}))))))

(defn ^:private node-id
  "Application-unique node-identifier of node `node`."
  [node]
  (case (stage node)
    :input (:input-id node)
    :output (:output-id node)
    #_else (error "node-id" node)))

(defn ^:private flatten-graph*
  [graph]
  (let [tails (if (vector? graph) graph [graph])]
    (->> (iterate (partial mapcat :requires) tails)
         (take-while seq)
         (apply concat)
         (cc/reduce
          (fn [[_ jids jobs :as state] node]
            (let [nid (node-id node), jid (jids nid)]
              (if jid
                state
                (let [jid (count jids), node (assoc node :jid jid)]
                  [tails (assoc jids nid jid) (conj jobs node)]))))
          [tails {} []]))))

(defn ^:private flatten-graph
  "Flatten job graph `graph` into a vector of job nodes annotated with vector
positional job ID as `:jid` and by-ID dependencies as `:requires`.  Return tuple
of the nodes-vector and a vector of the leaf-node job-IDs."
  [graph]
  (let [[tails jids jobs] (flatten-graph* graph)
        jid->rjid (partial - (-> jids count dec))
        rjids (comp jid->rjid jids), rjid (comp rjids node-id)]
    [(mapv (fn [{:keys [jid requires], :or {requires []}, :as node}]
             (let [jid (jid->rjid jid), requires (mapv rjid requires)]
               (assoc node :jid jid :requires requires)))
           (rseq jobs))
     (mapv rjid tails)]))

(defn ^:private job-name
  "Job name for `i`th job of `n` produced from var-name `base`."
  [base n i] (format "%s[%d/%d]" base (inc i) n))

(defn execute
  "Execute Hadoop jobs for the job graph `graph`, which should be a job graph
leaf node or vector of leaf nodes.  Jobs are configured starting with base
configuration `conf` and named based on the string `jname`.  Returns a vector of
the distributed sequences produced by the job graph leaves."
  [graph conf jname]
  (let [[nodes tails] (flatten-graph graph)
        njobs (- (count nodes) (count tails))
        job-name (partial job-name jname njobs)
        graph (->> nodes
                   (r/map (fn [{:keys [jid requires], :as node}]
                            (let [f (node-fn node conf (job-name jid))]
                              [jid [requires f]])))
                   (into {}))]
    (run-parallel graph tails)))
