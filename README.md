# Parkour

Hadoop MapReduce in idiomatic Clojure.  Parkour takes your Clojure code’s
functional gymnastics and sends it free-running across the urban environment of
your Hadoop cluster.

## Installation

Parkour is available on Clojars.  Add this `:dependency` to your Leiningen
`project.clj`:

```clj
[com.damballa/parkour "0.4.0"]
```

## Usage

Parkour is a Clojure library for writing Hadoop MapReduce jobs.  It tries to
avoid being a “framework” – if you know Hadoop, and you know Clojure, then
you’re most of the way to knowing Parkour.

The [Parkour introduction][intro] contains an overview of the key concepts, but
here is the classic “word count” example, in Parkour:

```clj
(defn mapper
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1])))))

(defn reducer
  [conf]
  (fn [context input]
    (->> (mr/keyvalgroups input)
         (r/map (fn [[word counts]]
                  [word (r/reduce + 0 counts)])))))

(defn word-count
  [dseq dsink]
  (-> (pg/input dseq)
      (pg/map #'mapper)
      (pg/partition [Text LongWritable])
      (pg/combine #'reducer)
      (pg/reduce #'reducer)
      (pg/output dsink)))
```

## Documentation

Parkour’s documentation is divided into a number of separate sections:

- [Introduction][intro] – A getting-started introduction, with an overview of
  Parkour’s key concepts.
- [Motivation][motivation] – An explanation of the goals Parkour exists to
  achieve, with comparison to other libraries and frameworks.
- [Namespaces][namespaces] – A tour of Parkour’s namespaces, explaining how each
  set of functionality fits into the whole.
- [Serialization][serialization] – How Parkour integrates Clojure with Hadoop
  serialization mechanisms.
- [Testing][testing] – Patterns for testing Parkour MapReduce jobs.
- [Deployment][deployment] – Running Parkour applications on a Hadoop cluster.
- [Reference][api] – Generated API reference, via [codox][codox].

## License

Copyright © 2013 Marshall Bockrath-Vandegrift & Damballa, Inc.

Distributed under the Apache License, Version 2.0.

[intro]: https://github.com/damballa/parkour/blob/master/doc/intro.md
[motivation]: https://github.com/damballa/parkour/blob/master/doc/motivation.md
[namespaces]: https://github.com/damballa/parkour/blob/master/doc/namespaces.md
[serialization]: https://github.com/damballa/parkour/blob/master/doc/serialization.md
[testing]: https://github.com/damballa/parkour/blob/master/doc/testing.md
[deployment]: https://github.com/damballa/parkour/blob/master/doc/deployment.md
[api]: https://github.atl.damballa/pages/rnd/parkour/
[codox]: https://github.com/weavejester/codox
