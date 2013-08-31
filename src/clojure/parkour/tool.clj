(ns parkour.tool
  (:require [parkour.conf :as conf]
            [parkour.util :refer [coerce]])
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
  ([f] (ParkourTool. nil f))
  ([conf f] (ParkourTool. (conf/iguration conf) f)))

(defn run
  "Run the Tool/function `t` with the arguments `args`."
  ([t args]
     (let [t (coerce Tool tool t)
           args (into-array String args)]
       (ToolRunner/run ^Tool t args)))
  ([conf t args]
     (let [conf (conf/iguration conf)
           t (coerce Tool tool t)
           args (into-array String args)]
       (ToolRunner/run ^Configuration conf ^Tool t args))))
