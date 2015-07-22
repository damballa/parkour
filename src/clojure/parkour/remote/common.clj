(ns parkour.remote.common
  {:private true}
  (:require [clojure.tools.logging :as log]
            [parkour (cser :as cser) (mapreduce :as mr)]
            [parkour.util :refer [returning]]))

(defn ^:private try-require*
  "Attempt to `require` namespace by symbol `ns`.  On success return `true` and
on failure log and return `false`."
  [ns]
  (try
    (returning true (require ns))
    (catch Throwable e
      (returning false
        (log/warnf e "%s: failed to load namespace." ns)))))

(defn ^:private ns-child-fn
  "Return function which returns true iff the provided namespace-symbol is a
child of namespace-symbol `prefix`."
 [prefix]
  (let [prefix (str (name prefix) ".")]
    (fn [sym] (.startsWith (name sym) prefix))))

(defn try-require
  "Load the namespaces `nses`.  On failure loading any particular namespace,
skip namespaces which are children of the failing namespace."
  [& nses]
  (loop [nses (sort nses)]
    (when-let [[ns & nses] (seq nses)]
      (if (try-require* ns)
        (recur nses)
        (recur (drop-while (ns-child-fn ns) nses))))))

(defn conf-require!
  "Load any namespaces specified by the value at `conf`'s \"parkour.namespaces\"
  key, skipping any which fail to load or are children of ones which fail to
  load, then replace the configuration value with the empty set."
  [conf]
  (apply try-require (cser/get conf "parkour.namespaces" #{}))
  (cser/assoc! conf "parkour.namespaces" #{}))

(defn adapt
  "Apply adapter function specified by var `v` via `::mr/adapter` metadata --
`default` if unspecified -- to the value of `v` and return resulting function."
  [default v]
  (let [m (meta v)
        w (if (::mr/raw m)
            identity
            (::mr/adapter m default))]
    (w (with-meta @v m))))
