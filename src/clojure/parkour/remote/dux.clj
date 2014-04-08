(ns parkour.remote.dux
  (:require [clojure.core.reducers :as r]
            [parkour (conf :as conf) (wrapper :as w) (mapreduce :as mr)]
            [parkour.io (dux :as dux)]
            [parkour.util :refer [map-vals prev-reset!]])
  (:import [clojure.lang IDeref]
           [org.apache.hadoop.mapreduce Job TaskAttemptContext]
           [org.apache.hadoop.mapreduce
             OutputFormat RecordWriter OutputCommitter]
           [parkour.hadoop IOutputFormat IRecordWriter IOutputCommitter]
           [parkour.hadoop
             Dux$OutputFormat Dux$RecordWriter Dux$OutputCommitter]))

(defn ^:private subjob
  [conf subconf] (-> conf conf/clone (conf/merge! subconf) mr/job))

(defn ^:private subof
  [^Job job] (->> job .getOutputFormatClass (w/new-instance job)))

(defn output-committer*
  {:tag `IOutputCommitter}
  [^TaskAttemptContext context jobs ofs rws]
  (let [taid (.getTaskAttemptID context)
        ocs (->> (keys jobs)
                 (map (fn [name]
                        (let [job (get jobs name)
                              tac (mr/tac job taid)
                              of (get ofs name)]
                          (.getOutputCommitter ^OutputFormat of tac))))
                 (zipmap (keys jobs)))]
    (reify
      IDeref (deref [_] [jobs ofs rws])
      IOutputCommitter
      (setupJob [_ context]
        (doseq [[name oc] ocs, :let [job (get jobs name)]]
          (.setupJob ^OutputCommitter oc ^Job job)))
      (commitJob [_ context]
        (doseq [[name oc] ocs, :let [job (get jobs name)]]
          (.commitJob ^OutputCommitter oc ^Job job)))
      (abortJob [_ context state]
        (doseq [[name oc] ocs, :let [job (get jobs name)]]
          (.abortJob ^OutputCommitter oc ^Job job state)))
      (setupTask [_ context]
        (let [taid (.getTaskAttemptID context)]
          (doseq [[name oc] ocs, :let [tac (-> jobs (get name) (mr/tac taid))]]
            (.setupTask ^OutputCommitter oc tac))))
      (needsTaskCommit [_ context]
        (let [taid (.getTaskAttemptID context)]
          (reduce (fn [_ [name oc]]
                    (let [tac (-> jobs (get name) (mr/tac taid))]
                      (if (.needsTaskCommit ^OutputCommitter oc tac)
                        (reduced true)
                        false)))
                  false ocs)))
      (commitTask [_ context]
        (let [taid (.getTaskAttemptID context)]
          (doseq [[name oc] ocs, :let [tac (-> jobs (get name) (mr/tac taid))]]
            (.commitTask ^OutputCommitter oc tac))))
      (abortTask [_ context]
        (let [taid (.getTaskAttemptID context)]
          (doseq [[name oc] ocs, :let [tac (-> jobs (get name) (mr/tac taid))]]
            (.abortTask ^OutputCommitter oc tac)))))))

(defn output-committer
  {:tag `OutputCommitter}
  [& args] (Dux$OutputCommitter. (apply output-committer* args)))

(defn record-writer*
  {:tag `IRecordWriter}
  [^TaskAttemptContext context]
  (reify IRecordWriter
    (write [_ key val]
      (throw (ex-info "Dux output tuple written to non-named output"
                      {:key key, :val val})))
    (close [_ context] #_pass)))

(defn record-writer
  {:tag `RecordWriter}
  [& args] (Dux$RecordWriter. (apply record-writer* args)))

(defn output-format*
  [conf]
  (let [diffs (->> conf mr/job dux/get-subconfs)
        jobs (map-vals (partial subjob conf) diffs)
        ofs (map-vals subof jobs)
        rws (atom {}), rwtaid (atom nil)]
    (reify IOutputFormat
      (getRecordWriter [_ context] (record-writer context))
      (checkOutputSpecs [_ context]
        (doseq [[name job] jobs, :let [of (get ofs name)]]
          (.checkOutputSpecs ^OutputFormat of ^Job job)))
      (getOutputCommitter [_ context]
        (output-committer context jobs ofs rws)))))
