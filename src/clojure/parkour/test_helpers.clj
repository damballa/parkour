(ns parkour.test-helpers
  (:require [parkour (conf :as conf)]))

(defn config
  "Configure `conf` (or fresh configuration if not provided) for local-mode test
execution."
  ([] (config (conf/ig)))
  ([conf]
     (conf/assoc! conf
       "fs.defaultFS" "file:///"
       "fs.default.name" "file:///"
       "mapred.job.tracker" "local"
       "jobclient.completion.poll.interval" 100)))

(defn config-fixture
  "A clojure.test fixture function for running tests under the local-mode test
configuration."
  [f] (conf/with-default (config) (f)))

(defmacro with-config
  "Execute `body` forms with the default configuration set to a local-mode test
configuration."
  [& body] `(conf/with-default (config) ~@body))
