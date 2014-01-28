(ns parkour.repl
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.nrepl :as nrepl]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)]
            [parkour.util :refer [returning]])
  (:import [java.io File Closeable]
           [org.apache.hadoop.filecache DistributedCache]))

(defn ^:private lein-home
  "Locate and return path of Leiningen home directory."
  []
  (as-> (System/getenv "LEIN_HOME") home
        (or (and home (io/file home))
            (io/file (System/getProperty "user.home") ".lein"))
        (.getAbsolutePath ^File home)))

(defn ^:private lein-repl-port
  "Read and return Leiningen nREPL server port."
  []
  (try
    (-> (lein-home) (io/file "repl-port") slurp Long/parseLong)
    (catch Exception e
      (throw (ex-info "Couldn't read port from ~/.lein/repl-port." {} e)))))

(defn ^:private project-file-path
  "Absolute path of the (CWD) project file."
  [] (-> "project.clj" io/file .getAbsolutePath))

(defn ^:private lein*
  "Evaluate `form` against Leiningen nREPL server."
  [form]
  (with-open [conn ^Closeable (nrepl/connect :port (lein-repl-port))]
    (-> (nrepl/client conn 60000)
        (nrepl/message {:op "eval", :code (pr-str form)})
        nrepl/response-values
        first)))

(defn ^:private jobjar-forms
  "Generate forms necessary to build job JAR via Leiningen nREPL server."
  []
  `(do
     (require 'leiningen.core.project
              'leiningen.core.classpath
              'leiningen.with-profile
              'leiningen.jar)
     (let [project# (leiningen.core.project/read
                     ~(project-file-path) [:default :jobjar])
           deps-profiles# (leiningen.with-profile/profiles-in-group
                           project# "-user,-provided,-dev,+jobjar")
           deps-project# (leiningen.core.project/set-profiles
                          project# deps-profiles#)]
       {:dep-paths (->> (leiningen.core.classpath/get-classpath deps-project#)
                        (filter #(.endsWith % ".jar")))
        :jobjar-path (get (leiningen.jar/jar project#) [:extension "jar"])})))

(defn ^:private user-home
  "User HDFS home directory."
  ([] (user-home (conf/ig)))
  ([conf] (-> conf (fs/path-fs ".") .getHomeDirectory)))

(defn ^:private pathize
  "Extract only the path component of path `p`."
  [p] (-> p fs/uri .getPath fs/path))

(defn cache-jars!
  "Copy JARs from local Maven-repository `jar-paths` to HDFS cache at `m2-path`,
updating `conf` to add the cached JARs to the job classpath."
  ([conf jar-paths]
     (let [m2-path (fs/path (user-home conf) ".m2/repository")]
       (cache-jars! conf m2-path jar-paths)))
  ([conf m2-path jar-paths]
     (returning conf
       (let [m2-path (fs/path m2-path), fs (fs/path-fs conf m2-path)]
         (doseq [src-path jar-paths
                 :let [dst-path (->> (str/split src-path #"/repository/" 2)
                                     second (fs/path m2-path))]]
           (when-not (fs/path-exists? fs dst-path)
             (io/copy (io/file src-path) dst-path :fs fs))
           (DistributedCache/addFileToClassPath
            (pathize dst-path) conf fs))))))

(defn launch!
  "Ensure job JAR and dependencies are up-to-date.  Derive a Hadoop
configuration from `conf` and configure it to use the project job JAR and
dependencies.  Apply function `f` to the resulting configuration followed by
`args` and return the result."
  [conf f & args]
  (let [{:keys [jobjar-path dep-paths]} (lein* (jobjar-forms))
        conf (doto (conf/ig conf)
               (conf/assoc! "mapred.jar" jobjar-path)
               (cache-jars! dep-paths))]
    (conf/with-default conf
      (apply f conf args))))
