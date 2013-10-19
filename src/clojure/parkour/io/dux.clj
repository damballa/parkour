(ns parkour.io.dux
  (:require [clojure.edn :as edn]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (sink :as snk)]
            [parkour.graph (cstep :as cstep) (dseq :as dseq) (dsink :as dsink)]
            [parkour.io (mux :as mux)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang IFn]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce Job TaskInputOutputContext]
           [org.apache.hadoop.mapreduce OutputFormat RecordWriter Counter]
           [parkour.hadoop Dux$OutputFormat]))

(def ^:private ^:const confs-key
  "parkour.dux.confs")

(defn ^:private dux-output?
  "True iff `job` is configured for demultiplex output."
  [job] (->> (conf/get-class job "mapreduce.outputformat.class" nil)
             (identical? Dux$OutputFormat)))

(defn ^:private dux-empty
  "Clone of `job` with empty demultiplex sub-configurations map."
  [job] (-> job mr/job (conf/assoc! confs-key "{}")))

(defn get-subconfs
  "Get map of `job` demultiplex sub-configuration diffs."
  [job]
  (or (if (dux-output? job)
        (some->> (conf/get job confs-key) (edn/read-string)))
      {}))

(defn add-subconf
  "Add demultiplex output `subconf` to `job` as `name`."
  [^Job job name subconf]
  (let [diff (-> job (conf/diff subconf) (dissoc confs-key))
        diffs (-> (get-subconfs job) (assoc name diff))]
    (doto job
      (.setOutputKeyClass Object)
      (.setOutputValueClass Object)
      (.setOutputFormatClass Dux$OutputFormat)
      (conf/assoc! confs-key (pr-str diffs)))))

(defn add-substep
  "Add configuration changes produced by `step` as a demultiplex
sub-configuration of `job`."
  [^Job job name step]
  (add-subconf job name (-> job dux-empty (cstep/apply! step))))

(defn dsink
  "Demultiplexing distributed sink, for other distributed sinks `named-dsinks`,
which should consist of alternating name/dsink pairs.  The distributed sequence
of the resulting sink is the multiplex distributed sequence of all component
sinks' sequences."
  [& named-dsinks]
  (let [dsinks (partition 2 named-dsinks)]
    (dsink/dsink
     (apply mux/dseq (map (comp dseq/dseq second) dsinks))
     (fn [^Job job]
       (reduce (partial apply add-substep) job dsinks)))))

(defn ^:private dux-state
  "Extract demultiplexing output state from `context`."
  [^TaskInputOutputContext context]
  @(.getOutputCommitter context))

(defn ^:private set-output-name
  "Re-implementation of `FileOutputFormat/setOutputName`."
  [job base] (conf/assoc! job "mapreduce.output.basename" base))

(defn ^:private get-counter
  "Get dux counter for output `oname`."
  {:tag `Counter}
  [^TaskInputOutputContext context oname]
  (.getCounter context "Demultiplexing Output" (name oname)))

(defn ^:private new-rw
  "Return new demultiplexing output sink for output `oname` and file output
basename `base`."
  [context oname base]
  (let [[jobs ofs rws] (dux-state context)
        of (get ofs oname), ^Job job (get jobs oname)
        conf (-> job conf/clone (cond-> base (set-output-name base)))
        tac (mr/tac conf context), c (get-counter context oname)
        ckey (.getOutputKeyClass job), cval (.getOutputValueClass job)
        rw (.getRecordWriter ^OutputFormat of tac)]
    (reify
      Configurable
      (getConf [_] conf)

      w/Wrapper
      (unwrap [_] rw)

      snk/TupleSink
      (-key-class [_] ckey)
      (-val-class [_] ckey)
      (-emit-keyval [_ key val]
        (.write rw key val)
        (.increment c 1))

      IFn
      (invoke [sink keyval] (snk/emit-keyval sink keyval))
      (invoke [sink key val] (snk/emit-keyval sink key val))
      (applyTo [sink args]
        (case (count args)
          1 (let [[keyval] args] (snk/emit-keyval sink keyval))
          2 (let [[key val] args] (snk/emit-keyval sink key val)))))))

(defn get-sink
  "Get sink for named output `oname` and optional (file output format only) file
basename `base`."
  ([context oname] (get-sink context oname nil))
  ([context oname base]
     (let [[jobs ofs rws] (dux-state context), rwkey [oname base]]
       @(or (get-in @rws rwkey)
            (let [new-rw (partial new-rw context oname base)
                  add-rw (fn [rws]
                           (if rws
                             (if-let [rw (get-in rws rwkey)]
                               rw
                               (assoc-in rws rwkey (delay (new-rw))))))]
              (-> rws (swap! add-rw) (get-in rwkey)))))))

(defn write
  "Write `key` and `val` to named output `oname` and optional (file output
format only) file basename `base`."
  ([context oname key val] (write context oname nil key val))
  ([context oname base key val]
     (-> context (get-sink oname base) (snk/emit-keyval key val))))
