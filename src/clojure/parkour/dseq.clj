(ns parkour.dseq
  (:require [parkour (conf :as conf) (mapreduce :as mr)]))

(defprotocol DSeqConfigure
  (-input [this job] "Modify Hadoop `job` to consume this dseq as input.")
  (-output [this job] "Modify Hadoop `job` to produce this dseq as output."))
