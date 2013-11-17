(ns parkour.remote.basic
  {:private true}
  (:require [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
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
  (let [conf (doto (conf/ig context)
               (conf/assoc! "parkour.step" "map"))
        [v args] (step-v-args conf "mapper" id)
        split (.getInputSplit ^MapContext context)]
    (log/infof "mapper: split=%s, var=%s, args=%s"
               (pr-str split) (pr-str v) (pr-str args))
    (conf/with-default conf
      (let [;; TODO: For 0.5.0, change default to `mr/collfn`
            f (adapt mr/contextfn v)
            g (apply f conf args)]
        (g context)))))

(defn reducer-run
  [id context]
  (let [step (conf/get context (str "parkour.reducer." id ".step"))
        conf (doto (conf/ig context)
               (conf/assoc! "parkour.step" step))
        [v args] (step-v-args conf "reducer" id)]
    (log/infof "reducer: var=%s, args=%s" (pr-str v) (pr-str args))
    (conf/with-default conf
      (let [;; TODO: For 0.5.0, change default to `mr/collfn`
            f (adapt mr/contextfn v)
            g (apply f conf args)]
        (g context)))))

(defn partitioner-set-conf
  [conf]
  (let [[v args] (step-v-args conf "partitioner")]
    (log/infof "partitioner: var=%s, args=%s" (pr-str v) (pr-str args))
    (conf/with-default conf
      (let [;; TODO: For 0.5.0, change default to `(comp mr/partfn constantly)`
            f (adapt mr/partfn v)]
        (apply f conf args)))))
