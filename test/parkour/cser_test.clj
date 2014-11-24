(ns parkour.cser-test
  (:require [clojure.test :refer :all]
            [parkour (conf :as conf) (cser :as cser)]
            [parkour.test-helpers :as th]))

(def conf-key
  "parkour.cser-test")

(defn roundtrips-conf?
  [x]
  (let [conf (conf/ig)]
    (cser/assoc! conf conf-key x)
    (= x (cser/get conf conf-key))))

(defn roundtrip-raw
  [x]
  (let [conf (conf/ig)]
    (cser/read-string conf (cser/pr-str conf x))))

(defn roundtrips-raw?
  [x] (= x (roundtrip-raw x)))

(deftest test-cser
  (th/with-config
    (are [x] (and (roundtrips-conf? x)
                  (roundtrips-raw? x))
         (range 10)
         [:foo 'bar "baz"]
         {:key #'val}
         java.lang.String)
    (let [re #"regular"]
      (is (= (str re) (str (roundtrip-raw re)))))))
