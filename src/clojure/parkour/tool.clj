(ns parkour.tool
  (:require [parkour.util :refer [coerce]])
  (:import [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.util Tool ToolRunner]))

(deftype ParkourTool [^:unsynchronized-mutable ^Configuration conf, f]
  Configurable
  (getConf [_] conf)
  (setConf [_ conf'] (set! conf conf'))

  Tool
  (run [_ args]
    (let [rv (apply f conf (seq args))]
      (cond (integer? rv) rv, rv 0, :else -1))))

(defn tool
  "Hadoop `Tool` for function `f`, which should take a Hadoop
`Configuration` as it's first argument, follow by any number of
additional (command-line string) argument."
  {:tag `Tool}
  [f] (ParkourTool. nil f))

(defn run
  "Run the Tool/function `t` with the arguments `args`."
  [t args] (ToolRunner/run (coerce Tool tool t) (into-array String args)))
