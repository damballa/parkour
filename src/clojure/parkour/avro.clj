(ns parkour.avro
  (:require [abracad.avro :as avro]
            [parkour.wrapper :as w]
            [parkour.util :refer [returning]])
  (:import [org.apache.avro.mapred AvroKey AvroValue AvroWrapper]
           [abracad.avro ClojureDatumReader ClojureDatumWriter]))

(extend-protocol w/Wrapper
  AvroWrapper
  (unwrap [w] (.datum w))
  (rewrap [w x] (returning w (.datum w x))))
