(ns parkour.remote.basic
  {:private true}
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [pjstadig.scopes :as s]
            [parkour (conf :as conf) (mapreduce :as mr) (wrapper :as w)])
  (:import [clojure.lang IFn$OOLL Var]
           [org.apache.hadoop.mapreduce MapContext]))

(defn require-readers
  "Require the namespaces of all `*data-readers*` vars."
  [] (doseq [[_ ^Var v] *data-readers*] (-> v .-ns ns-name require)))

(defn step-v-args
  ([conf key]
     (let [fqname (conf/get conf (str "parkour." key ".var"))
           [ns sym] (str/split fqname #"/" 2)
           ns (symbol (if-not (.startsWith ^String ns "#'") ns (subs ns 2)))
           v (do (require ns) (ns-resolve ns (symbol sym)))
           args (do (require-readers)
                    (some->> (conf/get conf (str "parkour." key ".args"))
                             (edn/read-string {:readers *data-readers*})))]
       [v args]))
  ([conf kind id]
     (step-v-args conf (str kind "." id))))

(defn adapt
  "Apply adapter function specified by var `v` via `::mr/adapter` metadata --
`default` if unspecified -- to the value of `v` and return resulting function."
  [default v]
  (let [m (meta v)
        w (if (::mr/raw m)
            identity
            (::mr/adapter m default))]
    (w @v)))

(defn mapper-run
  [id context]
  (s/with-resource-scope
    (let [conf (doto (conf/ig context)
                 (conf/assoc! "parkour.step" "map"))
          [v args] (step-v-args conf "mapper" id)
          split (.getInputSplit ^MapContext context)]
      (log/infof "mapper: var=%s, args=%s, split=%s"
                 (pr-str v) (pr-str args) (pr-str split))
      (conf/with-default conf
        (let [f (adapt mr/collfn v)
              g (apply f conf args)]
          (g context))))))

(defn reducer-run
  [id context]
  (s/with-resource-scope
    (let [step (conf/get context (str "parkour.reducer." id ".step"))
          conf (doto (conf/ig context)
                 (conf/assoc! "parkour.step" step))
          [v args] (step-v-args conf "reducer" id)]
      (log/infof "%sr: var=%s, args=%s" (name step) (pr-str v) (pr-str args))
      (conf/with-default conf
        (let [f (adapt mr/collfn v)
              g (apply f conf args)]
          (g context))))))

(defn partitioner-set-conf
  [conf]
  (let [[v args] (step-v-args conf "partitioner")]
    (log/infof "partitioner: var=%s, args=%s" (pr-str v) (pr-str args))
    (conf/with-default conf
      (let [f (adapt (comp mr/partfn constantly) v)]
        (apply f conf args)))))
