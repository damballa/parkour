(ns parkour.multiplex-test
  (:require [clojure.test :refer :all]
            [clojure.core.reducers :as r]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
                     (wrapper :as w) (graph :as pg)]
            [parkour.io (dseq :as dseq) (mux :as mux) (empty :as empty)
                        (text :as text) (avro :as mravro)]
            [parkour.test-helpers :as th]))

(use-fixtures :once th/config-fixture)

(deftest test-input
  (let [text (text/dseq "dev-resources/word-count-input.txt")
        avro (mravro/dseq [:default] "dev-resources/words.avro")
        multi (mux/dseq text avro)]
    (is (= {[:text "apple"]  3, [:text "banana"]  2, [:text "carrot"]  1,
            [:avro "applez"] 3, [:avro "bananaz"] 2, [:avro "carrotz"] 1}
           (->> multi
                (r/map (fn [[k v]] (if v [:text v] [:avro k])))
                (into [])
                (frequencies))))))

(deftest test-malkovich-malkovich
  (let [text (text/dseq "dev-resources/word-count-input.txt")
        avro (mravro/dseq [:default] "dev-resources/words.avro")
        multi (mux/dseq (mux/dseq text) (mux/dseq avro))]
    (is (= {[:text "apple"]  3, [:text "banana"]  2, [:text "carrot"]  1,
            [:avro "applez"] 3, [:avro "bananaz"] 2, [:avro "carrotz"] 1}
           (->> multi
                (r/map (fn [[k v]] (if v [:text v] [:avro k])))
                (into [])
                (frequencies))))))

(deftest test-union-empty
  (is (= #{"apple" "applez" "banana" "bananaz" "carrot" "carrotz"}
         (->> (mux/dseq (text/dseq "dev-resources/word-count-input.txt")
                        (text/dseq)
                        (mravro/dseq [:default] "dev-resources/words.avro")
                        (mravro/dseq [:default])
                        (empty/dseq))
              (r/map (fn [[k v]] (or v k)))
              (into #{})))))
