# Parkour

Hadoop MapReduce in idiomatic Clojure.  Parkour takes your Clojure code’s
functional gymnastics and sends it free-running across the urban environment of
your Hadoop cluster.

## Installation

Parkour is available on Clojars.  Add this `:dependency` to your Leiningen
`project.clj`:

```clj
[com.damballa/parkour "0.3.2"]
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

- [Introduction][intro] – A short getting-started introduction, with an overview
  of Parkour’s key concepts.
- [Motivation][motivation] – A detailed explanation of the goals Parkour exists
  to achieve, with comparison to other libraries and frameworks.
- [Namespaces guide][namespaces] – A guided tour of Parkour’s namespaces,
  explaining how each set of functionality fits into the whole.

## License

Copyright © 2013 Marshall Bockrath-Vandegrift & Damballa, Inc.

Distributed under the Eclipse Public License, the same as Clojure.

[intro]: blob/master/doc/intro.md
[motivation]: blob/master/doc/motivation.md
[namespaces]: blob/master/doc/namespaces.md
