(ns parkour.fs-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [parkour (conf :as conf) (fs :as fs)]
            [parkour.test-helpers :as th]))

(use-fixtures :once th/config-fixture)

(deftest test-path
  (let [p #hadoop.fs/path "foo"]
    (is (identical? p (fs/path p))))
  (is (= #hadoop.fs/path "foo"
         (fs/path "foo")))
  (is (= #hadoop.fs/path "file:foo"
         (fs/path "file:foo")
         (fs/path (io/file "foo"))
         (fs/path (io/as-url (io/file "foo")))
         (fs/path (fs/uri (io/as-url (io/file "foo"))))))
  (is (= #hadoop.fs/path "http://www.example.com/path"
         (fs/path "http://www.example.com/path")
         (fs/path (io/as-url "http://www.example.com/path"))
         (fs/path (fs/uri (io/as-url "http://www.example.com/path"))))))

(deftest test-path-io
  (is (= (slurp (io/resource "word-count-input.txt"))
         (slurp (fs/path (io/resource "word-count-input.txt")))))
  (let [p (fs/path "tmp/example.txt")]
    (-> p fs/path-fs (.delete p))
    (spit p "foo bar baz")
    (is (= "foo bar baz"
           (slurp p)
           (slurp (io/file p))
           (slurp (fs/uri p))))))

(deftest test-path-copy
  (let [p (fs/path "tmp/file1.txt")
        f (io/file "tmp/file2.txt")
        i (-> "bar baz quux" .getBytes io/input-stream)]
    (-> p fs/path-fs (.delete p))
    (-> f fs/path-fs (.delete (fs/path f)))
    (io/copy i p) (io/copy p f)
    (is (= "bar baz quux" (slurp f)))))

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
