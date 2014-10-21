(ns parkour.remote.basic
  {:private true}
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [pjstadig.scopes :as s]
            [parkour (conf :as conf) (mapreduce :as mr) (wrapper :as w)
             ,       (cser :as cser)]
            [parkour.util :refer [ignore-errors returning]])
  (:import [clojure.lang IFn$OOLL Var]
           [org.apache.hadoop.mapreduce MapContext]))

(defn ^:private try-require*
  "Attempt to `require` namespace by symbol `ns`.  On success return `true` and
on failure log and return `false`."
  [ns]
  (try
    (returning true (require ns))
    (catch Throwable e
      (returning false
        (log/warnf e "%s: failed to load namespace." ns)))))

(defn ^:private ns-child-fn
  "Return function which returns true iff the provided namespace-symbol is a
child of namespace-symbol `prefix`."
 [prefix]
  (let [prefix (str (name prefix) ".")]
    (fn [sym] (.startsWith (name sym) prefix))))

(defn try-require
  "Load the namespaces `nses`.  On failure loading any particular namespace,
skip namespaces which are children of the failing namespace."
  [& nses]
  (loop [nses (sort nses)]
    (when-let [[ns & nses] (seq nses)]
      (if (try-require* ns)
        (recur nses)
        (recur (drop-while (ns-child-fn ns) nses))))))

(defn step-v-args
  "The tuple of (task function-var, args) for the task `key` (and optional `id`)
in `conf`. "
  ([conf key]
     (apply try-require (cser/get conf "parkour.namespaces"))
     (let [v (cser/get conf (str "parkour." key ".var"))
           args (cser/get conf (str "parkour." key ".args"))]
       [v args]))
  ([conf kind id]
     (step-v-args conf (str kind "." id))))

(defn adapt
  "Apply adapter function specified by var `v` via `::mr/adapter` metadata --
`default` if unspecified -- to the value of `v` and return resulting function."
  [default v]
  (let [m (meta v)
        w (if (::mr/raw m)
            identity
            (::mr/adapter m default))]
    (w (with-meta @v m))))

(defn with-task-ex*
  "Functional form of `with-task-ex` macro."
  [context f]
  (if-not (mr/local-runner? context)
    (f)
    (try
      (reset! mr/task-ex nil)
      (f)
      (catch Exception e
        (reset! mr/task-ex e)
        (throw e)))))

(defmacro with-task-ex
  "Evaluate `body` forms, storing any exception in the `mr/task-ex` atom prior
to rethrowing."
  [context & body]
  `(with-task-ex* ~context (^:once fn* [] ~@body)))

(defn mapper-run
  [id context]
  (with-task-ex context
    (s/with-resource-scope
      (binding [mr/*context* context]
        (let [conf (doto (conf/ig context)
                     (conf/assoc! "parkour.step" "map"))
              [v args] (step-v-args conf "mapper" id)
              split (.getInputSplit ^MapContext context)]
          (log/infof "mapper: var=%s, args=%s, split=%s"
                     (pr-str v) (pr-str args) (pr-str split))
          (conf/with-default conf
            (let [f (adapt mr/collfn v)
                  g (apply f conf args)]
              (g context))))))))

(defn reducer-run
  [id context]
  (with-task-ex context
    (s/with-resource-scope
      (binding [mr/*context* context]
        (let [step (conf/get context (str "parkour.reducer." id ".step"))
              conf (doto (conf/ig context)
                     (conf/assoc! "parkour.step" step))
              [v args] (step-v-args conf "reducer" id)]
          (log/infof "%sr: var=%s, args=%s"
                     (name step) (pr-str v) (pr-str args))
          (conf/with-default conf
            (let [f (adapt mr/collfn v)
                  g (apply f conf args)]
              (g context))))))))

(defn partitioner-set-conf
  [conf]
  (let [[v args] (step-v-args conf "partitioner")]
    (log/infof "partitioner: var=%s, args=%s" (pr-str v) (pr-str args))
    (conf/with-default conf
      (let [f (adapt (comp mr/partfn constantly) v)]
        (apply f conf args)))))
