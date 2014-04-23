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
[com.damballa/parkour "0.5.4"]
```

## Usage

The [Parkour introduction][intro] contains an overview of the key concepts, but
here is the classic “word count” example, in Parkour:

```clj
(defn mapper
  {::mr/source-as :vals}
  [input]
  (->> input
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))

(defn reducer
  {::mr/source-as :keyvalgroups}
  [input]
  (r/map (fn [[word counts]]
           [word (r/reduce + 0 counts)])
         input))

(defn word-count
  [conf workdir lines]
  (let [wc-path (fs/path workdir "word-count")
        wc-dsink (seqf/dsink [Text LongWritable] wc-path)]
    (-> (pg/input lines)
        (pg/map #'mapper)
        (pg/partition [Text LongWritable])
        (pg/combine #'reducer)
        (pg/reduce #'reducer)
        (pg/output wc-dsink)
        (pg/execute conf "word-count")
        first)))
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

Copyright © 2013-2014 Marshall Bockrath-Vandegrift & Damballa, Inc.

Distributed under the Apache License, Version 2.0.

The EDN reader implementation (`EdnReader.java`) is copyright © 2013 Rich
Hickey, and is distributed under the Eclipse Public License v1.0.

[intro]: https://github.com/damballa/parkour/blob/master/doc/intro.md
[motivation]: https://github.com/damballa/parkour/blob/master/doc/motivation.md
[namespaces]: https://github.com/damballa/parkour/blob/master/doc/namespaces.md
[repl]: https://github.com/damballa/parkour/blob/master/doc/repl.md
[mr-detailed]: https://github.com/damballa/parkour/blob/master/doc/mr-detailed.md
[serialization]: https://github.com/damballa/parkour/blob/master/doc/serialization.md
[reducers-vs-seqs]: https://github.com/damballa/parkour/blob/master/doc/reducers-vs-seqs.md
[testing]: https://github.com/damballa/parkour/blob/master/doc/testing.md
[deployment]: https://github.com/damballa/parkour/blob/master/doc/deployment.md
[api]: http://damballa.github.io/parkour/
[codox]: https://github.com/weavejester/codox
[mailing-list]: http://librelist.com/browser/parkour/
[issues]: https://github.com/damballa/parkour/issues
