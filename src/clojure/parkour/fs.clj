(ns parkour.fs
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (reducers :as pr)]
            [parkour.util :refer [ignore-errors returning map-vals mpartial]])
  (:import [java.net URI URL]
           [java.io File IOException InputStream OutputStream Reader Writer]
           [org.apache.hadoop.fs FileStatus FileSystem Path]
           [org.apache.hadoop.filecache DistributedCache]))

(defprotocol Coercions
  "Protocol for coercing objects to Hadoop `Path`s and `java.net.URI`s.  Logical
extension of `clojure.java.io/Coercions`."
  (^org.apache.hadoop.fs.Path ^:no-doc
    -path [x] "Coerce argument to a Path; private implementation.")
  (^java.net.URI ^:no-doc
    -uri [x] "Coerce argument to a URI; private implementation."))

(defmacro ^:private write-all
  [w & forms]
  (let [w (vary-meta w assoc :tag `Writer)]
    `(do ~@(map (fn [x] `(. ~w write ~(if (string? x) x `(str ~x)))) forms))))

(defn path?
  "True iff `x` is a Path."
  [x] (instance? Path x))

(defn path
  "Coerce argument(s) to a Path, resolving successive arguments against base."
  {:tag `Path}
  ([x] (if (path? x) x (-path x)))
  ([x y] (Path. ^Path (path x) (str y)))
  ([x y & more] (apply path (path x y) more)))

(defn path-array
  "Array of `Path`s for each entry in seq `ps`."
  {:tag "[Lorg.apache.hadoop.fs.Path;"}
  [ps] (into-array Path (map path ps)))

(defmethod print-method Path [x w] (write-all w "#hadoop.fs/path \"" x "\""))
(defmethod print-dup Path [x w] (write-all w "#hadoop.fs/path \"" x "\""))

(defn uri?
  "True iff `x` is a URI."
  [x] (instance? URI x))

(defn uri
  "Coerce argument(s) to a URI, resolving successive arguments against base."
  {:tag `URI}
  ([x] (if (uri? x) x (-uri x)))
  ([x y]
     (let [^URI x (uri x)]
       (-> x (.resolve (str (.getPath x) "/")) (.resolve (str y)))))
  ([x y & more] (apply uri (uri x y) more)))

(defmethod print-method URI [x w] (write-all w "#java.net/uri \"" x "\""))
(defmethod print-dup URI [x w] (write-all w "#java.net/uri \"" x "\""))

(defn path-fs
  "Hadoop filesystem for the path `p`."
  {:tag `FileSystem}
  ([p] (path-fs (conf/ig) p))
  ([conf p] (.getFileSystem (path p) (conf/ig conf))))

(extend-protocol Coercions
  String
  (-path [x]
    (if (.startsWith x "file:")
      (-path (io/file (subs x 5)))
      (Path. x)))
  (-uri [x]
    (let [uri (URI. x)]
      (condp = (.getScheme uri)
        "file" (.toURI (io/file uri))
        nil    (-uri (-path x))
        ,,,,,, uri)))

  Path
  (-path [x] x)
  (-uri [x]
    (let [fs (ignore-errors (path-fs x))]
      (-> x (cond-> fs (.makeQualified fs)) .toUri)))

  URI
  (-path [x] (Path. x))
  (-uri [x] x)

  URL
  (-path [x] (Path. (str (.toURI x))))
  (-uri [x] (.toURI x))

  File
  (-path [x] (Path. (str "file:" (.getAbsolutePath x))))
  (-uri [x] (.toURI x)))

(extend-protocol io/Coercions
  Path
  (as-file [x] (io/as-file (uri x)))
  (as-url [x] (.toURL (uri x))))

(defn path-glob
  "Expand path glob `p` to set of matching paths."
  ([p] (let [p (path p)] (path-glob (path-fs p) p)))
  ([fs p]
     (->> (.globStatus ^FileSystem fs (path p))
          (map #(.getPath ^FileStatus %)))))

(defn path-list
  "List the entries in the directory at path `p`."
  ([p] (let [p (path p)] (path-list (path-fs p) p)))
  ([fs p]
     (->> (.listStatus ^FileSystem fs (path p))
          (map #(.getPath ^FileStatus %) ))))

(defn path-delete
  "Recursively delete files at path `p`."
  ([p] (let [p (path p)] (path-delete (path-fs p) p)))
  ([fs p] (.delete ^FileSystem fs (path p) true)))

(defn path-exists?
  ([p] (let [p (path p)] (path-exists? (path-fs p) p)))
  ([fs p] (.exists ^FileSystem fs (path p))))

(defn hidden?
  "True iff `p` is an HDFS-hidden ('_'-prefixed) file."
  [p] (-> p path .getName (.startsWith "_")))

(defn path-map
  "Return map of file basename to full path for all files in `dir`."
  ([dir] (let [dir (path dir)] (path-map (path-fs dir) dir)))
  ([fs dir]
     (let [dir (path dir)]
       (->> (.listStatus ^FileSystem fs dir)
            (r/remove #(.isDir ^FileStatus %))
            (r/map (comp (juxt #(.getName ^Path %) str)
                         #(.getPath ^FileStatus %)))
            (into {})))))

(defn path-open
  "Open an input stream on `p`, or default `fs` if not provided."
  ([p] (path-open (path-fs p) p))
  ([fs p] (.open ^FileSystem fs (path p))))

(defn input-stream
  "Open an input stream on `p`, via a Hadoop filesystem when in a
supported scheme and via `io/input-stream` when not."
  {:tag `InputStream}
  ([p] (input-stream (conf/ig) p))
  ([conf p]
     (let [p (path p)]
       (if-let [fs (ignore-errors (path-fs conf p))]
         (path-open fs p)
         (io/input-stream (io/as-url p))))))

(defn path-create
  "Create an output stream on `p` for `fs`, or default `fs` if not provided."
  ([p] (let [p (path p)] (path-create (path-fs p) p)))
  ([fs p] (.create ^FileSystem fs (path p))))

(extend Path
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream
    , (fn [p opts]
        (if-let [fs (or (:fs opts) (ignore-errors (path-fs p)))]
          (io/make-input-stream (path-open fs p) opts)
          (io/make-input-stream (io/as-url p) opts)))
    :make-output-stream
    , (fn [p opts]
        (if-let [fs (or (:fs opts) (ignore-errors (path-fs p)))]
          (io/make-output-stream (path-create fs p) opts)
          (io/make-output-stream (io/as-url p) opts)))))

;; Private for some reason, so copy internally
(def ^:private do-copy @#'io/do-copy)

(defmethod do-copy [Path OutputStream]
  [input output opts] (do-copy (io/input-stream input) output opts))
(defmethod do-copy [Path Writer]
  [input output opts] (do-copy (io/reader input) output opts))
(defmethod do-copy [Path File]
  [input output opts] (do-copy (io/input-stream input) output opts))

(defmethod do-copy [InputStream Path]
  [input output opts]
  (with-open [output (io/output-stream output)]
    (do-copy input output opts)))
(defmethod do-copy [Reader Path]
  [input output opts]
  (with-open [output (io/writer output)]
    (do-copy input output opts)))
(defmethod do-copy [File Path]
  [input output opts]
  (with-open [output (io/output-stream output)]
    (do-copy input output opts)))
(defmethod do-copy [URL Path]
  [input output opts]
  (with-open [input (io/input-stream input)
              output (io/output-stream output)]
    (do-copy input output opts)))
(defmethod do-copy [URI Path]
  [input output opts]
  (with-open [input (io/input-stream input)
              output (io/output-stream output)]
    (do-copy input output opts)))

(defmethod do-copy [Path Path]
  [input output opts]
  (with-open [output (io/output-stream output)]
    (do-copy input output opts)))

(def ^:dynamic *temp-dir*
  "Default path to system temporary directory on the default
filesystem.  May be overridden in configuration via the property
`parkour.temp.dir`."
  "/tmp")

(defn ^:private run-id
  "A likely-unique user- and time-based string."
  []
  (let [user (System/getProperty "user.name")
        time (System/currentTimeMillis)
        rand (rand-int Integer/MAX_VALUE)]
    (str user "-" time "-" rand)))

(defn ^:private temp-root
  [conf] (path (conf/get conf "parkour.temp.dir" *temp-dir*)))

(defn ^:private new-temp-dir
  "Return path for a new temporary directory."
  [conf] (path (temp-root conf) (run-id)))

(def ^:private cancel-doe-method?
  "True iff the `FileSystem` class has a static factory method."
  (->> FileSystem reflect/type-reflect :members
       (some #(= 'cancelDeleteOnExit (:name %)))))

(defmacro ^:private cancel-doe
  "Macro to cancel delete-on-exit when available."
  [& args]
  (if cancel-doe-method?
    `(.cancelDeleteOnExit ~@args)))

(defn with-temp-dir*
  "Function version of `with-temp-dir`."
  ([f] (with-temp-dir* nil f))
  ([conf f]
     (let [conf (or conf (conf/ig))
           temp-dir (new-temp-dir conf)
           fs (path-fs conf temp-dir)]
       (.mkdirs fs temp-dir)
       (.deleteOnExit fs temp-dir)
       (try
         (f temp-dir)
         (finally
           (.delete fs temp-dir true)
           (cancel-doe fs temp-dir))))))

(defmacro with-temp-dir
  "Run the forms in `body` with `temp-dir` bound to a Hadoop filesystem
temporary directory path, created via the optional `conf`."
  [[temp-dir conf] & body]
  `(with-temp-dir* ~conf
     (^:once fn* [d#] (let [~temp-dir d#] ~@body))))

(defn ^:private split-fragment
  [^URI uri]
  (let [fragment (.getFragment uri)]
    (if-not fragment
      [uri uri]
      (let [uri-s (str uri), n (count uri-s)
            base (subs uri-s 0 (- n (count fragment) 1))]
        [fragment (URI. base)]))))

(defn distcache-files
  "Retrieve map of existing distcache entries from `conf`."
  [conf]
  (->> (DistributedCache/getCacheFiles (conf/ig conf))
       (r/map split-fragment)
       (into {})))

(defn distcache!
  "Update Hadoop `conf` to merge the `uri-map` of local file paths to
URIs into the distributed cache configuration."
  [conf uri-map]
  (let [conf (doto (conf/ig conf)
               (DistributedCache/createSymlink))]
    (->> (into (distcache-files conf) uri-map)
         (map (fn [[local remote]]
                (if (identical? local remote)
                  remote
                  (.resolve (uri remote) (str "#" local)))))
         (str/join ",")
         (conf/assoc! conf "mapred.cache.files"))))

(defn distcacher
  "Return a function for merging the `uri-map` of local paths to URIs
into the distributed cache files of a Hadoop configuration."
  [uri-map] (mpartial distcache! uri-map))

(defn with-copies*
  "Function form of `with-copies`."
  [conf uri-map f]
  (with-temp-dir [temp-dir conf]
    (let [conf (or conf (conf/ig)), uri-map (map-vals uri uri-map)
          temp-fs (path-fs conf temp-dir)
          uri-map (reduce (fn [result [local remote]]
                            (let [temp (path temp-dir local)]
                              (returning (assoc result local (uri temp))
                                (with-open [inf (input-stream conf remote),
                                            outf (.create temp-fs temp)]
                                  (io/copy inf outf)))))
                          {} uri-map)]
      (f uri-map))))

(defmacro with-copies
  "Copy the URI values of `uri-map` to a temporary directory on the
default Hadoop filesystem, optionally specified by `conf`.  Evaluate
`body` forms with `name` bound to the map of original `uri-map` keys
to new temporary paths."
  [[name uri-map conf] & body]
  `(with-copies* ~conf ~uri-map
     (^:once fn* [m#] (let [~name m#] ~@body))))
