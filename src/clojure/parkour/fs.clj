(ns parkour.fs
  (:require [clojure.java.io :as io])
  (:import [java.net URI]
           [java.io File]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileSystem Path]))

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

(defn uri
  "Coerce argument(s) to a URI, resolving successive arguments against base."
  {:tag `URI}
  ([x] (-uri x))
  ([x y]
     (let [x (-uri x)]
       (-> x (.resolve (str (.getPath x) "/")) (.resolve (str y)))))
  ([x y & more] (apply uri (uri x y) more)))

(defn path-fs
  "Hadoop filesystem for the path `p`."
  {:tag `FileSystem}
  ([p] (path-fs (Configuration.) p))
  ([conf p] (.getFileSystem (path p) ^Configuration conf)))

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

  File
  (-path [x] (Path. (str "file:" (.getAbsolutePath x))))
  (-uri [x] (.toURI x)))
