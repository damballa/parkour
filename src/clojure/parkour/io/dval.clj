(ns parkour.io.dval
  (:require [clojure.java.io :as io]
            [parkour (fs :as fs) (cser :as cser) (reducers :as pr)]
            [parkour.io.transient :refer [transient-path]]
            [parkour.util :as util :refer [doto-let]])
  (:import [java.io Writer]
           [java.net URI]
           [clojure.lang IDeref IPending]
           [org.apache.hadoop.filecache DistributedCache]))

;; Distributed value
(defrecord DVal [value readv dcm]
  Object
  (toString [_]
    (let [v (if (realized? value) @value :pending)]
      (str "#<DVal: " (pr-str v) ">")))
  IDeref (deref [_] @value)
  IPending (isRealized [_] (realized? value)))

(defn ^:private distcache-dval
  "Remote-side dval data reader, reconstituting as a delay."
  [[readv cnames]]
  (let [remotes (DistributedCache/getCacheFiles cser/*conf*)
        locals (DistributedCache/getLocalCacheFiles cser/*conf*)
        ->entry (fn [r l] [(.getFragment ^URI r) l])
        cname->local (into {} (map ->entry remotes locals))
        sources (map cname->local cnames)]
    (delay (apply readv sources))))

(defmethod print-method DVal
  [^DVal dval ^Writer writer]
  (let [repr (if (nil? cser/*conf*)
               (str dval)
               (let [{:keys [readv dcm]} dval
                     literal (pr-str [readv (-> dcm keys vec)])]
                 (fs/distcache! cser/*conf* dcm)
                 (str "#parkour/dval " literal)))]
    (.write writer repr)))

(defn ^:private identity-ref
  "Return reference which yields `x` when `deref`ed."
  [x]
  (reify
    IDeref (deref [_] x)
    IPending (isRealized [_] true)))

(defn ^:private cache-name
  "Generate distinct distcache-entry name for `source`."
  [source] (str (gensym "dval-") "-" (-> source fs/path .getName)))

(defn dval
  "Return a dval which locally holds `value` and remotely will deserialize by
applying var `readv` to distributed copies of `sources`."
  [value readv & sources]
  (let [dcm (into {} (map (juxt cache-name fs/uri) sources))
        value (if (instance? IDeref value) value (identity-ref value))]
    (DVal. value readv dcm)))

(defn load-dval
  "Return a dval which will deserialize by applying var `readv` to `sources`
locally and distributed copies of `sources` remotely."
  [readv & sources]
  (let [value (delay (apply readv sources))]
    (apply dval value readv sources)))

(defn copy-dval
  "Like `load-dval`, but first copy `sources` to transient locations."
  [readv & sources]
  (let [sources (map fs/uri sources)
        sources (doto-let [tpaths (map transient-path sources)]
                  (doseq [[source tpath] (map vector sources tpaths)]
                    (io/copy source tpath)))]
    (apply load-dval readv sources)))

(defn transient-dval
  "Serialize `value` to a transient location by calling `writef` with the path
and `value`.  Return a dval which locally holds `value` and remotely will
deserialize by calling var `readv` with a distributed copy of the transient
serialization path."
  [writef readv value]
  (let [source (doto (transient-path) (writef value))]
    (dval value readv source)))

(defn edn-dval
  "EDN-serialize `value` to a transient location and yield a wrapping dval."
  [value] (transient-dval util/edn-spit #'util/edn-slurp value))
