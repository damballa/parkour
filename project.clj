(defproject com.damballa/parkour "0.6.2"
  :description "Hadoop MapReduce in idiomatic Clojure."
  :url "http://github.com/damballa/parkour"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :exclusions [org.apache.hadoop/hadoop-core
               org.apache.hadoop/hadoop-common
               org.apache.hadoop/hadoop-hdfs
               org.slf4j/slf4j-api org.slf4j/slf4j-log4j12 log4j
               org.apache.avro/avro
               org.apache.avro/avro-mapred
               org.apache.avro/avro-ipc]
  :dependencies [[org.clojure/tools.logging "0.3.1"]
                 [com.damballa/abracad "0.4.11"]
                 [org.apache.avro/avro "1.7.7"]
                 [org.platypope/letterpress "0.1.0"]
                 [pjstadig/scopes "0.3.0"]
                 [transduce/transduce "0.1.1"]]
  :plugins [[codox/codox "0.6.6"]]
  :codox {:src-dir-uri "https://github.com/damballa/parkour/blob/master/"
          :src-linenum-anchor-prefix "L"
          :sources ["src/clojure"]
          :exclude [parkour.io.dseq.mapred
                    parkour.io.dseq.mapreduce
                    parkour.mapreduce.sink
                    parkour.mapreduce.source
                    parkour.remote.basic
                    parkour.remote.dux
                    parkour.remote.mem
                    parkour.remote.mux
                    parkour.util
                    parkour.util.shutdown
                    parkour.util.shutdown.hadoop1
                    parkour.util.shutdown.hadoop2]
          :output-dir "tmp/codox"}
  :aliases {"few" ["with-profile"
                   ~(->> (for [c ["clojure-1-6-0"]
                               h ["hadoop-1-2-1" "hadoop-2-6-0"]]
                           ["default*" c h])
                         (map (partial clojure.string/join ","))
                         (clojure.string/join ":"))]
            "all" ["with-profile"
                   ~(->> (for [c ["clojure-1-5-1" "clojure-1-6-0"]
                               h ["hadoop-1-2-1" "hadoop-2-4-0" "hadoop-2-6-0"
                                  "hadoop-cdh4" "hadoop-cdh5"]]
                           ["default*" c h])
                         (map (partial clojure.string/join ","))
                         (clojure.string/join ":"))]}
  :profiles
  , {:default* [:base :system :user :provided :dev],
     :default+ ^:leaky [:clojure-1-6-0 :hadoop-stable],
     :default [:default* :default+]
     :conjars {:repositories
               [["conjars" "http://conjars.org/repo/"]]}
     :cloudera {:repositories
                [["cloudera" ~(str "https://repository.cloudera.com"
                                   "/artifactory/cloudera-repos/")]]}
     :provided [:slf4j-log4j :java6]
     :slf4j-log4j {:dependencies
                   [[org.slf4j/slf4j-api "1.6.1"]
                    [org.slf4j/slf4j-log4j12 "1.6.1"]
                    [log4j "1.2.17"]]}
     :java6 {:dependencies
             [[org.codehaus.jsr166-mirror/jsr166y "1.7.0"]]}
     :examples {:source-paths ["examples"]}
     :hadoop-user [:java6 :avro-cdh4 :clojure-1-6-0]
     :jobjar [:hadoop-user :examples]
     :dev [:examples
           :conjars ;; For cascading-* *
           {:dependencies
            [[alembic "0.2.1"]
             [cascading/cascading-hadoop "2.2.0"
              :exclusions [org.codehaus.janino/janino]]]}]
     :test {:resource-paths ["test-resources"]}
     :clojure-1-5-1 {:dependencies [[org.clojure/clojure "1.5.1"]]}
     :clojure-1-6-0 {:dependencies [[org.clojure/clojure "1.6.0"]]}
     :clojure-1-7-0 {:dependencies [[org.clojure/clojure "1.7.0-alpha5"]]}
     :hadoop-stable [:hadoop-2-6-0]
     :avro-hadoop1 {:dependencies
                    [[org.apache.avro/avro-mapred "1.7.7"]]}
     :hadoop-1-0-3 [:avro-hadoop1
                    {:dependencies
                     [[org.apache.hadoop/hadoop-core "1.0.3"]
                      [commons-io/commons-io "2.1"]]}]
     :hadoop-1-2-1 [:avro-hadoop1
                    {:dependencies
                     [[org.apache.hadoop/hadoop-core "1.2.1"]]}]
     :avro-hadoop2 ^{:pom-scope :provided}
     ,             {:dependencies
                    [[org.apache.avro/avro-mapred "1.7.7"
                      :classifier "hadoop2"]]}
     :hadoop-2-4-0 [:avro-hadoop2
                    {:dependencies
                     [[org.apache.hadoop/hadoop-client "2.4.0"]
                      [org.apache.hadoop/hadoop-common "2.4.0"]]}]
     :hadoop-2-6-0 [:avro-hadoop2 :client-2-6-0]
     :client-2-6-0 ^{:pom-scope :provided}
     ,             {:dependencies
                    [[org.apache.hadoop/hadoop-client "2.6.0"]
                     [org.apache.hadoop/hadoop-common "2.6.0"]]}
     :avro-cdh4 {:repositories
                 , [["platypope" "http://jars.platypope.org/release/"]]
                 :dependencies
                 , [[org.apache.avro/avro-mapred "1.7.7"
                     :classifier "hadoop1-cdh4"]]}
     :hadoop-cdh4 [:cloudera
                   :avro-cdh4
                   {:dependencies
                    [[org.apache.hadoop/hadoop-core "2.0.0-mr1-cdh4.1.2"]
                     [org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.2"]]}]
     :avro-cdh5 [:cloudera
                 {:dependencies
                  [[org.apache.avro/avro "1.7.6-cdh5.3.1"]
                   [org.apache.avro/avro-mapred "1.7.6-cdh5.3.1"
                    :classifier "hadoop2"]]}]
     :hadoop-cdh5 [:cloudera
                   :avro-cdh5
                   {:dependencies
                    [[org.apache.hadoop/hadoop-client "2.5.0-cdh5.3.1"]
                     [org.apache.hadoop/hadoop-common "2.5.0-cdh5.3.1"]]}]})
