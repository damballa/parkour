(ns parkour.io.dux
  (:require [clojure.edn :as edn]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.mapreduce (sink :as sink)]
            [parkour.graph (cstep :as cstep) (dseq :as dseq) (dsink :as dsink)]
            [parkour.io (mux :as mux)]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.conf Configurable]
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

(defn ^:private set-output-name
  "Re-implementation of `FileOutputFormat/setOutputName`."
  [job base] (conf/assoc! job "mapreduce.output.basename" base))

(defn ^:private get-counter
  "Get dux counter for output `oname`."
  {:tag `Counter}
  [^TaskInputOutputContext context oname]
  (.getCounter context "Demultiplexing Output" (name oname)))

(defn ^:private rw-sink
  [conf ckey cval ^Counter c ^RecordWriter rw]
  (reify
    Configurable
    (getConf [_] conf)

    w/Wrapper
    (unwrap [_] rw)

    sink/TupleSink
    (-key-class [_] ckey)
    (-val-class [_] ckey)
    (-emit-keyval [_ key val]
      (.write rw key val)
      (.increment c 1))))

(defn get-sink
  "Get sink for named output `name` and optional (file output format only) file
basename `base`."
  ([context name] (get-sink context name nil))
  ([^TaskInputOutputContext context name base]
     (let [[jobs ofs rws] @(.getOutputCommitter context)
           rwkey [name base]]
       @(or (get-in @rws rwkey)
            (let [new-rw (delay
                          (let [taid (.getTaskAttemptID context)
                                of (get ofs name), ^Job job (get jobs name)
                                tac (-> job conf/clone
                                        (cond-> base (set-output-name base))
                                        (mr/tac taid))]
                            (rw-sink
                             (conf/ig job)
                             (.getOutputKeyClass job)
                             (.getOutputValueClass job)
                             (get-counter context name)
                             (.getRecordWriter ^OutputFormat of tac))))
                  add-rw (fn [rws]
                           (if rws
                             (if-let [rw (get-in rws rwkey)]
                               rw
                               (assoc-in rws rwkey new-rw))))]
              (-> rws (swap! add-rw) (get-in rwkey)))))))

(defn write
  "Write `key` and `val` to named output `name` and optional (file output format
only) file basename `base`."
  ([context name key val] (write context name nil key val))
  ([context name base key val]
     (-> context (get-sink name base) (sink/emit-keyval key val))))
