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

(defn roundtrips-raw?
  [x]
  (let [conf (conf/ig)]
    (= x (cser/read-string conf (cser/pr-str conf x)))))

(deftest test-cser
  (th/with-config
    (let [conf (conf/ig)]
      (are [x] (and (roundtrips-conf? x)
                    (roundtrips-raw? x))
           (range 10)
           [:foo 'bar "baz"]
           {:key #'val})
      (are [x] (and (not (roundtrips-conf? x))
                    (not (roundtrips-raw? x)))
           ;; Any other failing cases?
           java.lang.String))))
