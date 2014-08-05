(ns parkour.example.word-count-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [parkour.tool :as tool]
            [parkour.test-helpers :as th]
            [parkour.example.word-count :as e]))

(deftest test-example-word-count
  (th/with-config
    (let [inpath (-> "word-count-input.txt" io/resource io/file str)]
      (is (= ["{\"apple\" 3, \"banana\" 2, \"carrot\" 1}"]
             (->> (tool/run e/tool [inpath])
                  (with-out-str)
                  (str/split-lines)
                  (sort)))))))
