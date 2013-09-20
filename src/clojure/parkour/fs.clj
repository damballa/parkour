(ns parkour.fs
  (:require [clojure.java.io :as io]
            [parkour (conf :as conf)])
  (:import [java.net URI URL]
           [java.io File]
           [org.apache.hadoop.fs FileStatus FileSystem Path]))

(defprotocol Coercions
  (^org.apache.hadoop.fs.Path
    -path [x] "Coerce argument to a Path; private implementation.")
  (^java.net.URI
    -uri [x] "Coerce argument to a URI; private implementation."))

(defn path
  "Coerce argument(s) to a Path, resolving successive arguments against base."
  {:tag `Path}
  ([x] (-path x))
  ([x y] (Path. (-path x) (str y)))
  ([x y & more] (apply path (path x y) more)))

(defmethod print-method Path
  ([^Path x ^java.io.Writer w]
     (.write w "#hadoop.fs/path \"")
     (.write w (str x))
     (.write w "\"")))

(defn uri
  "Coerce argument(s) to a URI, resolving successive arguments against base."
  {:tag `URI}
  ([x] (-uri x))
  ([x y]
     (let [x (-uri x)]
       (-> x (.resolve (str (.getPath x) "/")) (.resolve (str y)))))
  ([x y & more] (apply uri (uri x y) more)))

(defmethod print-method URI
  ([^URI x ^java.io.Writer w]
     (.write w "#java.net/uri \"")
     (.write w (str x))
     (.write w "\"")))

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
        nil    (let [p (Path. x)]
                 (.toUri (.makeQualified p (path-fs p))))
        ,,,,,, uri)))

  Path
  (-path [x] x)
  (-uri [x] (.toUri (.makeQualified x (path-fs x))))

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
  ([p] (path-glob (path-fs p) p))
  ([fs p]
     (->> (.globStatus ^FileSystem fs (path p))
          (map #(.getPath ^FileStatus %)))))

(defn path-list
  "List the entries in the directory at path `p`."
  ([p] (path-list (path-fs p) p))
  ([fs p]
     (->> (.listStatus ^FileSystem fs (path p))
          (map #(.getPath ^FileStatus %) ))))
