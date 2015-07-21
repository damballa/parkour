# Parkour

[![Build Status](https://secure.travis-ci.org/damballa/parkour.png)](http://travis-ci.org/damballa/parkour)

Hadoop MapReduce in idiomatic Clojure.  Parkour takes your Clojure code’s
functional gymnastics and sends it free-running across the urban environment of
your Hadoop cluster.

Parkour is a Clojure library for writing distributed programs in the MapReduce
pattern which run on the Hadoop MapReduce platform.  Parkour does its best to
avoid being yet another “framework” – if you know Hadoop, and you know Clojure,
then you’re most of the way to knowing Parkour.  By combining functional
programming, direct access to Hadoop features, and interactive iteration on live
data, Parkour supports rapid development of highly efficient Hadoop MapReduce
applications.

## Installation

Parkour is available on Clojars.  Add this `:dependency` to your Leiningen
`project.clj`:

```clj
[com.damballa/parkour "0.6.3"]
```

## Usage

The [Parkour introduction][intro] contains an overview of the key concepts, but
here is the classic “word count” example, in Parkour:

```clj
(defn word-count-m
  [coll]
  (->> coll
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))

(defn word-count
  [conf lines]
  (-> (pg/input lines)
      (pg/map #'word-count-m)
      (pg/partition [Text LongWritable])
      (pg/combine #'ptb/keyvalgroups-r #'+)
      (pg/output (seqf/dsink [Text LongWritable]))
      (pg/fexecute conf `word-count)))
```

## Documentation

Parkour’s documentation is divided into a number of separate sections:

- [Introduction][intro] – A getting-started introduction, with an overview of
  Parkour’s key concepts.
- [Motivation][motivation] – An explanation of the goals Parkour exists to
  achieve, with comparison to other libraries and frameworks.
- [Namespaces][namespaces] – A tour of Parkour’s namespaces, explaining how each
  set of functionality fits into the whole.
- [REPL integration][repl] – A quick guide to using Parkour from a
  cluster-connected REPL, for iterative development against live data.
- [MapReduce in depth][mr-detailed] – An in-depth examination of the interfaces
  Parkour uses to run your code in MapReduce jobs.
- [Serialization][serialization] – How Parkour integrates Clojure with Hadoop
  serialization mechanisms.
- [Unified I/O][unified-io] – Unified collection-like local and distributed I/O
  via Parkour dseqs and dsinks.
- [Distributed values][dvals] – Parkour’s value-oriented interface to the Hadoop
  distributed cache.
- [Multiple I/O][multi-io] – Configuring multiple inputs and/or outputs for
  single Hadoop MapReduce jobs.
- [Reducers vs seqs][reducers-vs-seqs] – Why Parkour’s default idiom uses
  reducers, and when to use seqs instead.
- [Testing][testing] – Patterns for testing Parkour MapReduce jobs.
- [Deployment][deployment] – Running Parkour applications on a Hadoop cluster.
- [Reference][api] – Generated API reference, via [codox][codox].

## Contributing

There is a [Parkour mailing list][mailing-list] hosted by
[Librelist](http://librelist.com/) for announcements and discussion.  To join
the mailing list, just email parkour@librelist.org; your first message to that
address will subscribe you without being posted.  Please report issues on the
[GitHub issue tracker][issues].  And of course, pull requests welcome!

## License

Copyright © 2013-2015 Marshall Bockrath-Vandegrift & Damballa, Inc.

Distributed under the Apache License, Version 2.0.

[intro]: https://github.com/damballa/parkour/blob/master/doc/intro.md
[motivation]: https://github.com/damballa/parkour/blob/master/doc/motivation.md
[namespaces]: https://github.com/damballa/parkour/blob/master/doc/namespaces.md
[repl]: https://github.com/damballa/parkour/blob/master/doc/repl.md
[mr-detailed]: https://github.com/damballa/parkour/blob/master/doc/mr-detailed.md
[serialization]: https://github.com/damballa/parkour/blob/master/doc/serialization.md
[unified-io]: https://github.com/damballa/parkour/blob/master/doc/unified-io.md
[dvals]: https://github.com/damballa/parkour/blob/master/doc/dvals.md
[multi-io]: https://github.com/damballa/parkour/blob/master/doc/multi-io.md
[reducers-vs-seqs]: https://github.com/damballa/parkour/blob/master/doc/reducers-vs-seqs.md
[testing]: https://github.com/damballa/parkour/blob/master/doc/testing.md
[deployment]: https://github.com/damballa/parkour/blob/master/doc/deployment.md
[api]: http://damballa.github.io/parkour/
[codox]: https://github.com/weavejester/codox
[mailing-list]: http://librelist.com/browser/parkour/
[issues]: https://github.com/damballa/parkour/issues
