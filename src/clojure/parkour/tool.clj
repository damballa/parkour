(ns parkour.tool
  (:require [clojure.tools.logging :as log]
            [parkour.conf :as conf]
            [parkour.util :refer [coerce returning]])
  (:import [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.util Tool ToolRunner]))

(defn integral*
  "Function form of the `integral` macro."
  [f]
  (try
    (let [rv (f)]
      (if (integer? rv) rv 0))
    (catch Exception e
      (returning -1
        (log/error e "Uncaught exception:" (str e))))))

(defmacro integral
  "Evaluate `body` forms.  If the result of evaluation is an integral value,
return it.  If the result is non-integral, return 0.  If evaluation throws an
exception, return -1."
  [& body] `(integral* (fn* ^:once [] ~@body)))

(deftype ParkourTool [^:unsynchronized-mutable ^Configuration conf, f]
  Configurable
  (getConf [_] conf)
  (setConf [_ conf'] (set! conf conf'))

  Tool
  (run [_ args]
    (integral
     (conf/with-default conf
       (apply f conf (seq args))))))

(defn tool
  "Hadoop `Tool` for function `f`, which should take a Hadoop `Configuration` as
its first argument, follow by any number of additional (command-line string)
argument."
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
