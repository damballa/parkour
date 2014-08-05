(ns parkour.example.matrixify-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [parkour.tool :as tool]
            [parkour.test-helpers :as th]
            [parkour.example.matrixify :as e]))

(deftest test-example-matrixify
  (th/with-config
    (let [inpath (-> "matrixify-input.txt" io/resource io/file str)]
      (is (= ["0 0 4.0" "1 2 5.0" "2 0 1.0" "2 1 2.0" "2 2 3.0" "3 2 6.0"]
             (->> (tool/run e/tool [inpath])
                  (with-out-str)
                  (str/split-lines)
                  (sort)))))))
