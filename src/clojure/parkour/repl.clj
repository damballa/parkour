(ns parkour.repl
  (:require [clojure.java.io :as io]
            [clojure.tools.nrepl :as nrepl]
            [parkour (conf :as conf) (mapreduce :as mr)])
  (:import [java.io File Closeable]))

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

(defn ^:private uberjar-forms
  "Forms to evaluate in Leiningen process to build uberjar."
  []
  `(-> ~(project-file-path)
       (leiningen.core.project/read)
       (leiningen.uberjar/uberjar)))

(defn ^:private lein-uberjar
  "Build and return path to uberjar for project."
  []
  (with-open [conn ^Closeable (nrepl/connect :port (lein-repl-port))]
    (-> (nrepl/client conn 60000)
        (nrepl/message {:op "eval", :code (pr-str (uberjar-forms))})
        nrepl/response-values
        first)))

(defn launch!
  "Ensure job JAR and dependencies are up-to-date.  Derive a Hadoop
configuration from `conf` and configure it to use the project job JAR and
dependencies.  Apply function `f` to the resulting configuration followed by
`args` and return the result."
  [conf f & args]
  (let [jar-path (lein-uberjar)
        conf (conf/assoc! (conf/ig conf)
               "mapred.jar" jar-path)]
    (conf/with-default conf
      (apply f conf args))))
