(ns parkour.io.dsink
  (:require [parkour (conf :as conf) (cser :as cser) (cstep :as cstep)
             ,       (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (sink :as snk)]
            [parkour.io (dseq :as dseq)]
            [parkour.util :refer [ignore-errors returning coerce]])
  (:import [java.io Closeable Writer]
           [clojure.lang IObj]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce OutputCommitter OutputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]))

(deftype DSink [meta dseq step]
  Object
  (toString [this]
    (str "conf=" (-> step cstep/step-map pr-str ignore-errors (or "?"))))

  IObj
  (meta [_] meta)
  (withMeta [_ meta] (DSink. meta dseq step))

  cstep/ConfigStep
  (-apply! [_ job] (cstep/apply! job step))

  dseq/DSeqable
  (-dseq [_] dseq))

(defn dsink?
  "True iff `x` is a distributed sink."
  [x] (instance? DSink x))

(defn dsink
  "Return distributed sink represented by job configuration step `step` and
producing the distributed sequence `dseq` once generated.  Result is a config
step which returns `dseq` when called as a zero-argument function.  When `dseq`
is not provided, and `step` is not already a distributed sink, the resulting
sink produces the `nil` dseq."
  ([step] (if (dsink? step) step (dsink nil step)))
  ([dseq step] (DSink. (meta step) dseq step)))

(defn dsink-dseq
  "Force `step` to a dsink, then return its corresponding dseq."
  [step] (-> step dsink dseq/dseq))

(defn sink-for
  "Local sink for writing tuples as written via `dsink`.  Must `.close` to
flush, as via `with-open`."
  {:tag `Closeable}
  [dsink]
  (let [job (cstep/apply! dsink), conf (conf/ig job), tac (mr/tac conf)
        ckey (.getOutputKeyClass job), cval (.getOutputValueClass job)
        of (doto ^OutputFormat (w/new-instance job (.getOutputFormatClass job))
                 (.checkOutputSpecs job))
        oc (doto (.getOutputCommitter of tac)
             (.setupJob job)
             (.setupTask tac))
        rw (.getRecordWriter of tac)]
    (snk/wrap-sink
     (reify
       Configurable (getConf [_] conf)
       w/Wrapper (unwrap [_] rw)
       snk/TupleSink
       (-key-class [_] ckey)
       (-val-class [_] cval)
       (-emit-keyval [_ key val] (.write rw key val))
       (-close [_]
         (.close rw tac)
         (when (.needsTaskCommit oc tac)
           (.commitTask oc tac))
         (.commitJob oc job))))))

(defn with-dseq*
  "Function form of `with-dseq`."
  [dsink f]
  (returning (dsink-dseq dsink)
    (with-open [sink (sink-for dsink)]
      (mr/sink sink (f)))))

(defmacro with-dseq
  "Evaluate `body` forms, write tuples from resulting collection to the local
sink produced from `dsink`, and return `dsink`'s associated dseq."
  [dsink & body] `(with-dseq* ~dsink (^:once fn* [] ~@body)))

(def output-paths* nil)
(defmulti ^:internal output-paths*
  "Internal implementation multimethod for `output-paths`."
  {:arglists '([job])}
  #(.getOutputFormatClass ^Job %))

(defn output-paths
  "Vector of output-paths produced by job or configuration step `step`."
  [step] (->> step (coerce Job cstep/apply!) output-paths*))

(defmethod output-paths* :default
  [_] [])

(defmethod output-paths* FileOutputFormat
  [^Job job] [(FileOutputFormat/getOutputPath job)])

(defn set-default-shape!
  "Set default sink shape for `conf` to `shape`."
  [conf shape] (cser/assoc! conf "parkour.sink-as.default" shape))
