# Testing Pakour jobs

Like most Hadoop libraries, Pakour supports testing jobs by running jobs in
local mode.  Unlike most libraries, local access to dseqs and dsinks makes
input-construction and result-checking nearly as straightforward as testing
non-MapReduce code.

## Configuration

Local-mode tests require a local-mode Hadoop configuration, which should
override the default configuration in e.g. a cluster-connected REPL.  In the
`parkour.test-helpers` namespace Parkour provides a `with-config` macro and
`config-fixture` `clojure.test`-style fixture-function for scoping tests to such
a configuration.  They provide a dynamic scope in which a test-optimized
local-only Hadoop configuration becomes the default and a dynamic scope for
cleaning transient resources.  All Parkour application tests should run under
one of these facilities.

```clj
(require '[parkour.test-helpers :as th])

(use-fixtures :once th/config-fixture)

;; or

(deftest test-something
  (th/with-config
    ...))
```

## Input/Output

Parkour allows seamless testing of job results exactly as a regular program
would consume job results.  Parkour does provide a small number of test-support
functions primarily designed to simplify construction of test _inputs_.

### with-dseq

The `parkour.io.dsink` namespace `with-dseq` function writes a Clojure
collection to a dsink, using the same backing Hadoop `OutputFormat` the dsink
configures an actual Hadoop job to use.  The function then yields the associated
dseq, which may be used as input for a job or job graph.

```clj
(deftest test-word-count-local
  (th/with-config
    (let [inpath (doto (fs/path "tmp/word-count-input") fs/path-delete)
          outpath (doto (fs/path "tmp/word-count-output") fs/path-delete)
          dseq (dsink/with-dseq (text/dsink inpath)
                 (->> ["apple banana banana" "carrot apple" "apple"]
                      (mr/sink-as :keys)))
          dsink (seqf/dsink [Text LongWritable] outpath)
          [result] (word-count (conf/ig) dseq dsink)]
      (is (= {"apple" 3, "banana" 2, "carrot" 1}
             (into {} result))))))
```

### mem/dseq

The `parkour.io.mem` namespace `dseq` function generates dseqs which use a
memory-backed Hadoop `InputFormat` to produce tuples from arbitrary in-memory
Clojure collections.  This circumvents normal Hadoop serialization, but can
simplify testing of jobs normally consuming from more esoteric and/or non-HDFS
input formats.

```clj
(deftest test-word-count-mem
  (th/with-config
    (let [outpath (doto (fs/path "tmp/word-count-output") fs/path-delete)
          dseq (->> [["apple banana banana"]
                     ["carrot apple"]
                     ["apple"]]
                    (mem/dseq :keys))
          dsink (seqf/dsink [Text LongWritable] outpath)
          [result] (word-count (conf/ig) dseq dsink)]
      (is (= {"apple" 3, "banana" 2, "carrot" 1}
             (into {} result))))))
```

## Partitioners

Hadoop local mode only allows a single reducer, which prevents any job custom
partitioners from even running.  Although not ideal, any custom partitioners
should be unit-tested directly, independently of job execution.  Fortunately,
Parkour partitioners are just functions, and relatively easy to so-test.
