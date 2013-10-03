(ns parkour.mapreduce.tasks
  {:private true}
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [parkour (conf :as conf)]
            [parkour.util :refer [doto-let]]))

(defn ^:private step-f-args
  ([conf key]
     (let [fqname (conf/get conf (str "parkour." key ".var"))
           [ns sym] (str/split fqname #"/" 2)
           ns (if-not (.startsWith ^String ns "#'") ns (subs ns 2))
           f (ns-resolve (symbol ns) (symbol sym))
           args (some->> (conf/get conf (str "parkour." key ".args"))
                         (edn/read-string {:readers *data-readers*}))]
       [f args]))
  ([conf kind id]
     (step-f-args conf (str kind "." id))))

(defn mapper-run
  [id context]
  (let [conf (doto (conf/ig context)
               (conf/assoc! "parkour.step" "map"))
        [f args] (step-f-args conf "mapper" id)]
    (conf/with-default conf
      ((apply f conf args) context))))

(defn reducer-run
  [id context]
  (let [conf (doto-let [conf (conf/ig context)]
               (->> (conf/get conf (str "parkour.reducer." id ".step"))
                    (conf/assoc! conf "parkour.step")))
        [f args] (step-f-args conf "reducer" id)]
    (conf/with-default conf
      ((apply f conf args) context))))

(defn partitioner-set-conf
  [conf]
  (let [[f args] (step-f-args conf "partitioner")]
    (conf/with-default conf
      (apply f conf args))))
