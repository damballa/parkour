(ns parkour.graph.tasks
  {:private true}
  (:require [parkour.mapreduce :as mr]
            [parkour.remote.graph :as prg]
            [parkour.util :refer [var-symbol]])
  (:import [org.apache.hadoop.mapreduce Job Mapper Partitioner Reducer]))

(defn task!
  [job node fkey klass alloc]
  (let [impl (fkey node)]
    (if (isa? impl klass)
      impl
      (let [{:keys [uvar rargs jid]} node]
        (alloc job #'prg/remote-task fkey (var-symbol uvar) rargs jid)))))

(defmacro deftask
  [fname fkey alloc klass method]
  `(defn ~fname
     [~(with-meta 'job {:tag `Job}) ~'node]
     (when (~fkey ~'node)
       (. ~'job ~method (task! ~'job ~'node ~fkey ~klass ~alloc)))))

(deftask mapper! :mapper mr/mapper! Mapper setMapperClass)
(deftask combiner! :combiner mr/combiner! Reducer setCombinerClass)
(deftask partitioner!
  :partitioner mr/partitioner! Partitioner setPartitionerClass)
(deftask reducer! :reducer mr/reducer! Reducer setReducerClass)
