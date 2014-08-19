(ns parkour.io.dval
  (:require [clojure.java.io :as io]
            [parkour (fs :as fs) (cser :as cser) (mapreduce :as mr)
             ,       (reducers :as pr)]
            [parkour.io.transient :refer [transient-path]]
            [parkour.util :as util :refer [doto-let]])
  (:import [java.io Writer]
           [java.net URI]
           [clojure.lang IDeref IObj IPending]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.filecache DistributedCache]))

(defn ^:private cache-name
  "Generate distinct distcache-entry name for `source`."
  [source] (str (gensym "dval-") "-" (-> source fs/path .getName)))

(defn ^:private dcpath*
  [dcname path]
  (let [path (str (fs/path path))
        dcpath* (fn dcpath* [md]
                  (proxy [Path IObj] [path]
                    (meta [] md)
                    (withMeta [md] (dcpath* md))))]
    (dcpath* {:type ::dcpath, ::dcname dcname})))

(defn dcpath
  "Distributed-cache–able instance of `path`.  When serialized into a job
configuration, adds `path` to the distributed cache.  When deserialized from a
job configuration, reconstitutes as the cache path when available and as the
original remote path when not (i.e. under local- or mixed-mode job execution)."
  [path]
  (let [path (fs/path path)
        dcname (cache-name path)]
    (dcpath* dcname path)))

(defmethod print-method ::dcpath
  [path ^Writer writer]
  (if (nil? cser/*conf*)
    (print-method (with-meta path nil) writer)
    (let [^String dcname (-> path meta ::dcname)]
      (fs/distcache! cser/*conf* {dcname path})
      (.write writer "#parkour/dcpath \"")
      (.write writer dcname)
      (.write writer "\""))))

(defn ^:private unfragment
  "The provided `uri`, but without any fragment."
  {:tag `URI}
  [^URI uri]
  (let [fragment (.getFragment uri)]
    (if (nil? fragment)
      uri
      (let [uri-s (str uri), n (- (count uri-s) (count fragment) 1)]
        (fs/uri (subs uri-s 0 n))))))

(defn ^:private resolve-source
  "Resolve distributed cache `remote` and `local` URIs to the \"most local\"
available source path.  Result will usually be a local file path, but may be the
original remote path under mixed-mode job execution."
  [[^URI remote local]]
  (if-let [fragment (.getFragment remote)]
    (let [symlink (io/file fragment), symlink? (.exists symlink)
          local (io/file (str local)), local? (.exists local)
          remote (unfragment remote), remote? (= "file" (.getScheme remote))
          source (cond symlink? symlink, local? local, remote? remote
                       ;; Could localize, but issues: clean-up, directories
                       (mr/local-runner? cser/*conf*) remote
                       :else (throw (ex-info
                                     (str remote ": cannot locate local file")
                                     {:remote remote, :local local})))]
      (fs/path source))))

(defn ^:private dcpath-reader
  "EDN tagged-literal reader for dcpaths."
  [dcname]
  (let [remotes (seq (DistributedCache/getCacheFiles cser/*conf*))
        locals (or (seq (DistributedCache/getLocalCacheFiles cser/*conf*))
                   (map unfragment remotes))]
    (if (not= (count remotes) (count locals))
      (throw (ex-info "cache files do not match local files"
                      {:remotes remotes, :locals locals}))
      (->> (map vector remotes locals)
           (pr/ffilter (fn [[^URI r]] (= dcname (.getFragment r))))
           (resolve-source)
           (dcpath* dcname)))))

;; Distributed value
(deftype DVal [value form]
  Object
  (toString [_]
    (let [v (if (realized? value) @value :pending)]
      (str "#<DVal: " (pr-str v) ">")))
  IDeref (deref [_] @value)
  IPending (isRealized [_] (realized? value)))

(defmethod print-method DVal
  [^DVal dval ^Writer w]
  (if (nil? cser/*conf*)
    (.write w (str dval))
    (do
      (.write w "#parkour/dval ")
      (.write w (pr-str (.-form dval))))))

(defn ^:private dval-reader
  "EDN tagged-literal reader for dvals."
  [[f & args]] (delay (apply f args)))

(defn ^:private dval*
  "Return a dval which locally proxies to `valref` and remotely will deserialize
as a delay over applying var `readv` to `args`."
  [valref readv & args] (DVal. valref (cons readv args)))

(defn dval
  "Return a dval which acts as a delay over applying var `readv` to `args`."
  [readv & args] (apply dval* (delay (apply readv args)) readv args))

(defn ^:private identity-ref
  "Return reference which yields `x` when `deref`ed."
  [x] (reify IDeref (deref [_] x), IPending (isRealized [_] true)))

(defn value-dval
  "Return a dval which locally holds `value` and remotely will deserialize as a
delay over applying var `readv` to `args`."
  [value readv & args] (apply dval* (identity-ref value) readv args))

(defn load-dval
  "Return a delay-like dval which will realize by applying var `readv` to the
concatenation of `params` and `sources` locally and distributed copies of
`sources` remotely."
  ([readv sources] (load-dval readv nil sources))
  ([readv params sources]
     (apply dval readv (concat params (map dcpath sources)))))

(defn copy-dval
  "Like `load-dval`, but first copy `sources` to transient locations."
  ([readv sources] (copy-dval readv nil sources))
  ([readv params sources]
     (let [sources (map fs/uri sources)
           sources (doto-let [tpaths (map transient-path sources)]
                     (doseq [[source tpath] (map vector sources tpaths)]
                       (io/copy source tpath)))]
       (load-dval readv params sources))))

(defn transient-dval
  "Serialize `value` to a transient location by calling `writef` with the path
and `value`.  Return a dval which locally holds `value` and remotely will
deserialize by calling var `readv` with a distributed copy of the transient
serialization path."
  [writef readv value]
  (let [source (doto (transient-path) (writef value))]
    (value-dval value readv (dcpath source))))

(defn edn-dval
  "EDN-serialize `value` to a transient location and yield a wrapping dval."
  [value] (transient-dval util/edn-spit #'util/edn-slurp value))

(defn jser-dval
  "Java-serialize `value` to a transient location and yield a wrapping dval."
  [value] (transient-dval util/jser-spit #'util/jser-slurp value))
