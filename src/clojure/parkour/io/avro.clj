(ns parkour.io.avro
  (:refer-clojure :exclude [shuffle])
  (:require [abracad.avro :as avro]
            [parkour (conf :as conf) (fs :as fs) (wrapper :as w)
                     (mapreduce :as mr)]
            [parkour.io (dseq :as dseq) (dsink :as dsink)]
            [parkour.util :refer [ignore-errors returning mpartial]])
  (:import [org.apache.avro Schema]
           [org.apache.avro.mapred AvroKey AvroValue AvroWrapper]
           [org.apache.avro.mapreduce
             AvroJob AvroKeyInputFormat AvroKeyValueInputFormat
             AvroKeyOutputFormat AvroKeyValueOutputFormat]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.mapreduce.lib.input FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output FileOutputFormat]
           [abracad.avro ClojureData]
           [parkour.hadoop AvroKeyGroupingComparator]))

(extend-protocol w/Wrapper
  AvroWrapper
  (unwrap [w] (.datum w))
  (rewrap [w x] (returning w (.datum w x))))

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
       (.setInputFormatClass AvroKeyInputFormat)))
  ([^Job job ks vs]
     (when-not (identical? :default ks)
       (AvroJob/setInputKeySchema job (avro/parse-schema ks)))
     (when-not (identical? :default vs)
       (AvroJob/setInputValueSchema job (avro/parse-schema vs)))
     (doto job
       (set-data-model)
       (.setInputFormatClass AvroKeyValueInputFormat))))

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
       (set-map-output ks)
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
       (.setOutputValueClass NullWritable)))
  ([^Job job ks vs]
     (doto job
       (set-data-model)
       (.setOutputFormatClass AvroKeyValueOutputFormat)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setOutputValueSchema (avro/parse-schema vs)))))

(defn tuple-schema
  "Return Avro schema for record consisting of fields of the provided `types`."
  [& types]
  (let [schemas (mapv avro/parse-schema types)
        schemas (->> {:name (name (gensym "parkour.io.avro.tuple")),
                      :type "record",
                      :abracad.reader "vector",
                      :fields (map-indexed (fn [i ^Schema schema]
                                             {:name (str "field" i),
                                              :type (.getFullName schema)})
                                           schemas)}
                     (conj schemas))]
    (apply avro/parse-schema schemas)))

(defn dseq
  "Distributed sequence of Avro input, applying the vector of `schemas` as per
the arguments to `set-input`, and reading from `paths`."
  [schemas & paths]
  (dseq/dseq
   (fn [^Job job]
     (apply set-input job schemas)
     (doseq [path paths]
       (FileInputFormat/addInputPath job (fs/path path))))))

(defn shuffle
  "Configuration step for Avro shuffle, with key schema `ks`, optional value
schema `vs`, and optional grouping schema `gs`."
  ([ks] (mpartial set-map-output ks))
  ([ks vs] (mpartial set-map-output ks vs))
  ([ks vs gs]
     [(mpartial set-map-output ks vs)
      (mpartial set-grouping gs)]))

(defn dsink
  "Distributed sink for Avro output, applying the vector of `schemas` as per the
arguments to `set-output`, and writing to `path`."
  [schemas path]
  (dsink/dsink
   (let [defaults (-> schemas count (repeat :default))]
     (dseq defaults path))
   (fn [^Job job]
     (apply set-output job schemas)
     (FileOutputFormat/setOutputPath job (fs/path path)))))
