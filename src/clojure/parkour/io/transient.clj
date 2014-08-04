(ns parkour.io.transient
  (:require [pjstadig.scopes :as s]
            [parkour (conf :as conf) (fs :as fs)]
            [parkour.util :as util :refer [ignore-errors doto-let]])
  (:import [org.apache.hadoop.fs Path]))

(defonce
  ^{:private true
    :doc "Process-wide run identifier for transient FS entries."}
  run-id
  (str (util/run-id) "-parkour-transient"))

(defn ^:private transient-root
  "Transient path root directory, as specified by `conf` if provided."
  {:tag `Path}
  ([] (transient-root (conf/ig)))
  ([conf]
     (-> (or (conf/get conf "parkour.transient.dir")
             (fs/path (fs/temp-root conf) run-id))
         (fs/path))))

(defn transient-path
  "Return new unique transient path, which will be deleted on process exit or
when leaving the current resource scope.  If `p` is provided and not `nil`,
should be a path, and returned path will have the same path name.  If `conf` is
provided, it will be used to determine the transient path root directory."
  ([] (transient-path nil))
  ([p] (transient-path (conf/ig) p))
  ([conf p]
     (let [root (transient-root conf), fs (fs/path-fs conf root)
           tbase (fs/path root (name (gensym "t-")))
           tpath (cond-> tbase p (fs/path (-> p fs/path .getName)))]
       (.mkdirs fs root)
       (.deleteOnExit fs root)
       (when (bound? #'s/*resources*)
         (s/scoped! tbase #(ignore-errors (fs/path-delete fs %))))
       tpath)))
