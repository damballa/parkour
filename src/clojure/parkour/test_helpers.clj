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
