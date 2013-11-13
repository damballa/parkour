(ns parkour.test-helpers
  (:require [parkour (conf :as conf)]))

(defn config
  ([] (config (conf/ig)))
  ([conf]
     (conf/assoc! conf
       "jobclient.completion.poll.interval" 100)))
