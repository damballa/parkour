(ns parkour.avro
  (:require [abracad.avro :as avro]
            [parkour.wrapper :as w]
            [parkour.mapreduce :as mr]
            [parkour.util :refer [returning]])
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

(defn task
  "Returns a function which calls `f` with an `unwrap`ed tuple source
then Avro-wraps and sinks all tuples from the resulting `reduce`able
collection."
  [f]
  (fn [input output]
    (let [output (mr/wrap-sink AvroKey AvroValue output)]
      (->> input w/unwrap f (mr/sink output)))))

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
       (.setMapOutputValueClass job NullWritable)))
  ([^Job job ks vs]
     (doto job
       (AvroJob/setMapOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setMapOutputValueSchema (avro/parse-schema vs)))))

(defn ^:private get-output-format
  "Retrieve `job`'s configure output format class as a string, or
`nil` if none has yet been specified."
  [^Job job] (-> job .getConfiguration (.get "mapreduce.outputformat.class")))

(defn set-output
    "Configure `job` output to produce Avro with key schema `ks` and
optional value schema `vs`.  Configures job output format to match
when the output format has not been otherwise explicitly specified."
  ([^Job job ks]
     (when (nil? (get-output-format job))
       (.setOutputFormatClass job AvroKeyOutputFormat))
     (doto job
       (set-data-model)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (.setOutputValueClass job NullWritable)))
  ([^Job job ks vs]
     (when (nil? (get-output-format job))
       (.setOutputFormatClass job AvroKeyValueOutputFormat))
     (doto job
       (set-data-model)
       (AvroJob/setOutputKeySchema (avro/parse-schema ks))
       (AvroJob/setOutputValueSchema (avro/parse-schema vs)))))
