(ns parkour.io.avro
  (:refer-clojure :exclude [shuffle])
  (:require [abracad.avro :as avro]
            [abracad.avro.edn :as aedn]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr) (reducers :as pr) (cser :as cser)]
            [parkour.io (dseq :as dseq) (dsink :as dsink) (dval :as dval)]
            [parkour.io.transient :refer [transient-path]]
            [parkour.util :refer [ignore-errors returning]])
  (:import [java.net URI]
           [org.apache.avro Schema]
           [org.apache.avro.mapred AvroKey AvroValue AvroWrapper FsInput]
           [org.apache.avro.mapreduce
             AvroJob AvroKeyInputFormat AvroKeyOutputFormat
             AvroKeyValueOutputFormat]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]
           [abracad.avro ClojureData]
           [parkour.hadoop
             AvroKeyGroupingComparator ClojureAvroKeyValueInputFormat]))

(extend-protocol w/Wrapper
  AvroWrapper
  (unwrap [w] (.datum w))
  (rewrap [w x] (returning w (.datum w x))))

(def ^:private fs-input-impl
  {:-seekable-input (fn [p opts]
                      (FsInput.
                       (fs/path p)
                       (cond (contains? opts :conf) (:conf opts)
                             (contains? opts :fs) (conf/ig (:fs opts))
                             :else (conf/ig))))})

(extend Path avro/PSeekableInput fs-input-impl)
(extend URI avro/PSeekableInput fs-input-impl)

(defn wrap-sink
  "Wrap task context for sinking Avro output."
  [context] (mr/wrap-sink AvroKey AvroValue context))

(defn task
  "Returns a function which calls `f` with an `unwrap`ed tuple source
and expects `f` to return a `reduce`able object.  Avro-wraps and sinks
all tuples from the resulting `reduce`able."
  [f]
  (fn [context]
    (let [output (wrap-sink context)]
      (->> context w/unwrap f (mr/sink output)))))

(defn ^:private set-data-model
  "Configure `job` to use the Abracad Clojure data model."
  [^Job job] (AvroJob/setDataModelClass job ClojureData))

(defn set-input
  "Configure `job` for Avro input with keys or keyvals using expected
schemas `ks` and `vs`.  Schemas may be `:default` to just directly use
input writer schema(s)."
  ([^Job job ks]
     (when-not (identical? :default ks)
       (AvroJob/setInputKeySchema job (avro/parse-schema ks)))
     (doto job
       (set-data-model)
       (.setInputFormatClass AvroKeyInputFormat)
       (dseq/set-default-shape! :keys)))
  ([^Job job ks vs]
     (when-not (identical? :default ks)
       (AvroJob/setInputKeySchema job (avro/parse-schema ks)))
     (when-not (identical? :default vs)
       (AvroJob/setInputValueSchema job (avro/parse-schema vs)))
     (doto job
       (set-data-model)
       (.setInputFormatClass ClojureAvroKeyValueInputFormat))))

(defn set-map-output
  "Configure `job` map output to produce Avro with key schema `ks` and
optional value schema `vs`."
  ([^Job job ks]
     (doto job
       (set-data-model)
       (AvroJob/setMapOutputKeySchema (avro/parse-schema ks))
       (.setMapOutputValueClass NullWritable)))
  ([^Job job ks vs]
     (if (nil? vs)
       (set-map-output job ks)
       (doto job
         (set-data-model)
         (AvroJob/setMapOutputKeySchema (avro/parse-schema ks))
         (AvroJob/setMapOutputValueSchema (avro/parse-schema vs))))))

(defn set-grouping
  "Configure `job` combine & reduce phases to group keys via schema `gs`,
which should be encoding-compatible with the map-output key schema."
  [^Job job gs]
  (AvroKeyGroupingComparator/setGroupingSchema job (avro/parse-schema gs)))

(defn ^:private get-output-format
  "Retrieve `job`'s configure output format class as a string, or
`nil` if none has yet been specified."
  [^Job job] (-> job .getConfiguration (.get "mapreduce.outputformat.class")))

(let [cname 'org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat
      exists? (-> cname str Class/forName ignore-errors boolean)]
  (defmacro ^:private lazy-output-format*
    [job klass]
    (if exists?
      `(. ~cname ~'setOutputFormatClass ~job ~klass)
      `(.setOutputFormatClass ~job ~klass))))

(defn ^:private lazy-output-format
  [job klass] (lazy-output-format* ^Job job ^Class klass))

(defn set-output
  "Configure `job` output to produce Avro with key schema `ks` and
optional value schema `vs`.  Configures job output format to match
when the output format has not been otherwise explicitly specified."
  ([^Job job ks]
     (doto job
       (set-data-model)
       (.setOutputFormatClass AvroKeyOutputFormat)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (.setOutputValueClass NullWritable)
       (dsink/set-default-shape! :keys)))
  ([^Job job ks vs]
     (doto job
       (set-data-model)
       (.setOutputFormatClass AvroKeyValueOutputFormat)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setOutputValueSchema (avro/parse-schema vs))))
  ([^Job job ks vs gs]
     (set-output job ks vs)))

(defn dseq
  "Distributed sequence of Avro input, applying the vector of `schemas` as per
the arguments to `set-input`, and reading from `paths`."
  [schemas & paths]
  (dseq/dseq
   (fn [^Job job]
     (apply set-input job schemas)
     (FileInputFormat/setInputPaths job (fs/path-array paths)))))

(defn ^:private shuffle*
  "Internal implementation for `shuffle`."
  ([ks] (pr/mpartial set-map-output ks))
  ([ks vs] (pr/mpartial set-map-output ks vs))
  ([ks vs gs]
     [(pr/mpartial set-map-output ks vs)
      (pr/mpartial set-grouping gs)]))

(defn shuffle
  "Configuration step for Avro shuffle, with key schema `ks`, optional value
schema `vs`, and optional grouping schema `gs`."
  {:arglists '([[ks]] [[ks vs]] [[ks vs gs]])}
  [schemas] (apply shuffle* schemas))

(defn dsink
  "Distributed sink for writing Avro data files at `path`, or a transient path
if not provided.  Configures output schemas as per application of `set-output`
to `schemas`."
  ([schemas] (dsink schemas (transient-path)))
  ([schemas path]
     (dsink/dsink
      (let [defaults (-> schemas count (repeat :default))]
        (dseq defaults path))
      (fn [^Job job]
        (apply set-output job schemas)
        (FileOutputFormat/setOutputPath job (fs/path path))))))

(defn dval
  "Avro distributed value, serialized using `schema` if provided or a plain
EDN-in-Avro schema if not."
  ([value] (dval (aedn/new-schema) value))
  ([schema value]
     (let [writef (partial avro/spit schema) ]
       (dval/transient-dval writef #'avro/slurp value))))
