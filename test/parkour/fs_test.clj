(ns parkour.fs-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [parkour (conf :as conf) (fs :as fs)]))

(deftest test-with-copies
  (let [input-uri (fs/uri (io/resource "word-count-input.txt"))]
    (fs/with-copies [{copied-uri "example"} {"example" input-uri}]
      (is (not= input-uri copied-uri))
      (is (= (slurp input-uri) (slurp copied-uri))))))

(deftest test-distcache
  (let [foo1 (fs/uri "/example/foo1")
        foo2 (fs/uri "/example/foo2")
        bar (fs/uri "/example/bar")
        conf (conf/ig)]
    (fs/distcache! conf {"foo" foo1, "bar" bar})
    (is (= {"foo" foo1, "bar" bar} (fs/distcache-files conf)))
    (fs/distcache! conf {"foo" foo2})
    (is (= {"foo" foo2, "bar" bar} (fs/distcache-files conf)))))
