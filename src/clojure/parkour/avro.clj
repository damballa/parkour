(ns parkour.avro
  (:require [abracad.avro :as avro]
            [parkour.wrapper :as w]
            [parkour.mapreduce :as mr]
            [parkour.util :refer [ignore-errors returning]])
  (:import [org.apache.avro.mapred AvroKey AvroValue AvroWrapper]
           [org.apache.avro.mapreduce
             AvroJob AvroKeyOutputFormat AvroKeyValueOutputFormat]
           [org.apache.hadoop.io NullWritable]
           [org.apache.hadoop.mapreduce Job]
           [abracad.avro ClojureData]))

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
    (let [output (mr/wrap-sink AvroKey AvroValue context)]
      (->> context w/unwrap f (mr/sink output)))))

(defn ^:private set-data-model
  "Configure `job` to use the Abracad Clojure data model."
  [^Job job] (AvroJob/setDataModelClass job ClojureData))

(defn set-map-output
  "Configure `job` map output to produce Avro with key schema `ks` and
optional value schema `vs`."
  ([^Job job ks]
     (doto job
       (set-data-model)
       (AvroJob/setMapOutputKeySchema (avro/parse-schema ks))
       (.setMapOutputValueClass NullWritable)))
  ([^Job job ks vs]
     (doto job
       (set-data-model)
       (AvroJob/setMapOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setMapOutputValueSchema (avro/parse-schema vs)))))

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
     (when (nil? (get-output-format job))
       (lazy-output-format job AvroKeyOutputFormat))
     (doto job
       (set-data-model)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (.setOutputValueClass NullWritable)))
  ([^Job job ks vs]
     (when (nil? (get-output-format job))
       (lazy-output-format job AvroKeyValueOutputFormat))
     (doto job
       (set-data-model)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setOutputValueSchema (avro/parse-schema vs)))))
