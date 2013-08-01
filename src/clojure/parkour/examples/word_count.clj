(ns parkour.examples.word-count
  (:require [clojure.string :as str]
            [clojure.core.reducers :as r]
            [abracad.avro :as avro]
            [parkour.mapreduce :as mr]
            [parkour.wrapper :as w]
            [parkour.fs :as fs]
            [parkour.avro :as mra]
            [parkour.reducers :as pr]
            [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.io IntWritable Text]
           [org.apache.hadoop.mapreduce.lib.input
             TextInputFormat FileInputFormat]
           [org.apache.hadoop.mapreduce.lib.output
             TextOutputFormat FileOutputFormat]
           [org.apache.avro.mapred AvroKey AvroValue]
           [org.apache.avro.mapreduce
             AvroJob AvroKeyOutputFormat AvroKeyValueOutputFormat]
           [abracad.avro ClojureDatumMapping]))

(defn arg0
  ([x] x)
  ([x y] x)
  ([x y z] x)
  ([x y z & more] x))

(defn arg1
  ([x y] y)
  ([x y z] y)
  ([x y z & more] y))

(defn avro-task
  ([f] (avro-task mr/keyvals mr/emit-keyval f))
  ([inputf f] (avro-task inputf mr/emit-keyval f))
  ([inputf emitf f]
     (fn [input output]
       (->> input (inputf w/unwrap) f
            (r/map (w/wrap-keyvals AvroKey AvroValue))
            (r/reduce emitf output)))))

(defn mapper
  [conf]
  (->> (fn [input]
         (->> (r/mapcat #(str/split % #"\s") input)
              (r/map #(-> [% 1]))))
       (avro-task mr/vals)))

(defn reducer
  [conf]
  (->> (partial r/map (pr/mjuxt identity (partial r/reduce +)))
       (avro-task mr/keyvalgroups)))

(defn -main
  [& args]
  (let [[inpath outpath] args, job (mr/job)]
    (doto job
      (mr/set-mapper-var #'mapper)
      (mr/set-combiner-var #'reducer)
      (mr/set-reducer-var #'reducer)
      (.setInputFormatClass TextInputFormat)
      (.setOutputFormatClass AvroKeyValueOutputFormat)
      (AvroJob/setDatumMappingClass ClojureDatumMapping)
      (AvroJob/setMapOutputKeySchema (avro/parse-schema :string))
      (AvroJob/setMapOutputValueSchema (avro/parse-schema :long))
      (AvroJob/setOutputKeySchema (avro/parse-schema :string))
      (AvroJob/setOutputValueSchema (avro/parse-schema :long))
      (FileInputFormat/addInputPath (fs/path inpath))
      (FileOutputFormat/setOutputPath (fs/path outpath)))
    (.waitForCompletion job true)))

(comment

  (defn complete-job
    [dseq]
    (mr/mapreduce
     (avro-task mr/vals
                (fn [input]
                  (->> (r/mapcat #(str/split % #"\s") input)
                       (r/map #(-> [% 1])))))
     (avro-task mr/keyvalgroups
                (partial r/map (fn [[word counts]]
                                 [word (r/reduce + counts)])))
     dseq))

  (defn word-count-job
    [dseq]
    (->> (r/map second dseq)
         (r/mapcat #(str/split % #"\n"))
         (r/map #(-> [% 1]))
         (mr/group)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)]))))

  )

