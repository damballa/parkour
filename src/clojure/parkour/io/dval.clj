(ns parkour.io.dval
  (:require [clojure.java.io :as io]
            [parkour (fs :as fs) (cser :as cser) (mapreduce :as mr)
             ,       (reducers :as pr)]
            [parkour.io.transient :refer [transient-path]]
            [parkour.util :as util :refer [doto-let]])
  (:import [java.io Writer]
           [java.net URI]
           [clojure.lang IDeref IPending]
           [org.apache.hadoop.filecache DistributedCache]))

;; Distributed value
(deftype DVal [value readv params dcm]
  Object
  (toString [_]
    (let [v (if (realized? value) @value :pending)]
      (str "#<DVal: " (pr-str v) ">")))
  IDeref (deref [_] @value)
  IPending (isRealized [_] (realized? value)))

(defn ^:private unfragment
  "The provided `uri`, but without any fragment."
  {:tag `URI}
  [^URI uri]
  (let [fragment (.getFragment uri)]
    (if (nil? fragment)
      uri
      (let [uri-s (str uri), n (- (count uri-s) (count fragment) 1)]
        (fs/uri (subs uri-s 0 n))))))

(defn ^:private ->entry
  "Parse `remote` and `local` into mapping tuple of (fragment, cache path)."
  [^URI remote local]
  (if-let [fragment (.getFragment remote)]
    (let [symlink (io/file fragment), symlink? (.exists symlink)
          local (io/file (str local)), local? (.exists local)
          remote (unfragment remote), remote? (= "file" (.getScheme remote))
          source (cond symlink? symlink, local? local, remote? remote
                       (mr/local-runner? cser/*conf*) remote
                       :else (throw (ex-info
                                     (str remote ": cannot locate local file")
                                     {:remote remote, :local local})))]
      [fragment (fs/path source)])))

(defn ^:private distcache-dval
  "Remote-side dval data reader, reconstituting as a delay."
  [[readv params cnames]]
  (let [remotes (seq (DistributedCache/getCacheFiles cser/*conf*))
        locals (seq (DistributedCache/getLocalCacheFiles cser/*conf*))
        _ (when (not= (count remotes) (count locals))
            (throw (ex-info "cache files do not match local files"
                            {:remotes remotes, :locals locals})))
        entries (map ->entry remotes locals)
        cname->source (->> entries (remove nil?) (into {}))
        sources (map cname->source cnames)
        args (concat params sources)]
    (delay (apply readv args))))

(defmethod print-method DVal
  [^DVal dval ^Writer writer]
  (let [repr (if (nil? cser/*conf*)
               (str dval)
               (let [v (.-readv dval), p (.-params dval), dcm (.-dcm dval)
                     literal (pr-str [v p (-> dcm keys vec)])]
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
applying var `readv` to the concatenation of `params` and distributed copies of
`sources`."
  ([value readv sources] (dval value readv nil sources))
  ([value readv params sources]
     (let [dcm (into {} (map (juxt cache-name fs/uri) sources))
           value (if (instance? IDeref value) value (identity-ref value))]
       (DVal. value readv params dcm))))

(defn load-dval
  "Return a dval which will deserialize by applying var `readv` to the
concatenation of `params` and `sources` locally and distributed copies of
`sources` remotely."
  ([readv sources] (load-dval readv nil sources))
  ([readv params sources]
     (let [args (concat params sources)
           value (delay (apply readv args))]
       (dval value readv params sources))))

(defn copy-dval
  "Like `load-dval`, but first copy `sources` to transient locations."
  ([readv sources] (copy-dval readv nil sources))
  ([readv params sources]
     (let [sources (map fs/uri sources)
           sources (doto-let [tpaths (map transient-path sources)]
                     (doseq [[source tpath] (map vector sources tpaths)]
                       (io/copy source tpath)))]
       (apply load-dval readv params sources))))

(defn transient-dval
  "Serialize `value` to a transient location by calling `writef` with the path
and `value`.  Return a dval which locally holds `value` and remotely will
deserialize by calling var `readv` with a distributed copy of the transient
serialization path."
  [writef readv value]
  (let [source (doto (transient-path) (writef value))]
    (dval value readv [source])))

(defn edn-dval
  "EDN-serialize `value` to a transient location and yield a wrapping dval."
  [value] (transient-dval util/edn-spit #'util/edn-slurp value))
