(defproject com.damballa/parkour "0.5.0"
  :description "Hadoop MapReduce in idiomatic Clojure."
  :url "http://github.com/damballa/parkour"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.damballa/abracad "0.4.7"]
                 [org.apache.avro/avro "1.7.5"]
                 [org.apache.avro/avro-mapred "1.7.5"
                  :exclusions [org.apache.avro/avro-ipc]]]
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
                    parkour.util]
          :output-dir "tmp/codox"}
  :aliases {"test-all" ["with-profile" ~(str "+hadoop-0-20-205:"
                                             "+hadoop-1-0-3:"
                                             "+hadoop-1-2-1:"
                                             "+hadoop-cdh3:"
                                             "+hadoop-cdh4:"
                                             "+hadoop-2-2-0")
                        ,              "test"]}
  :profiles {:default [:base :system :user :provided :java6 :dev]
             :conjars {:repositories
                       [["conjars" "http://conjars.org/repo/"]]}
             :cloudera {:repositories
                        [["cloudera" ~(str "https://repository.cloudera.com"
                                           "/artifactory/cloudera-repos/")]]}
             :platypope {:repositories
                         [["platypope" "http://jars.platypope.org/release/"]]}
             :provided {:dependencies
                        [[org.apache.hadoop/hadoop-core "1.2.1"]
                         [org.slf4j/slf4j-api "1.6.1"]
                         [org.slf4j/slf4j-log4j12 "1.6.1"]
                         [log4j "1.2.17"]]}
             :java6 {:dependencies
                     [[org.codehaus.jsr166-mirror/jsr166y "1.7.0"]]}
             :examples {:source-paths ["examples"]}
             :dev [:examples
                   :conjars ;; For cascading-* *
                   {:dependencies
                    [[cascading/cascading-hadoop "2.2.0"
                      :exclusions [org.codehaus.janino/janino
                                   org.apache.hadoop/hadoop-core]]]}]
             :test {:resource-paths ["test-resources"]}
             :hadoop-0-20-205 {:dependencies
                               [[org.apache.hadoop/hadoop-core "0.20.205.0"]]}
             :hadoop-1-0-3 {:dependencies
                            [[org.apache.hadoop/hadoop-core "1.0.3"]]}
             :hadoop-1-2-1 {:dependencies
                            [[org.apache.hadoop/hadoop-core "1.2.1"]]}
             :hadoop-cdh3 [:cloudera ;; For hadoop-* *-*cdh3*
                           {:dependencies
                            [[org.apache.hadoop/hadoop-core "0.20.2-cdh3u3"]]}]
             :hadoop-cdh4 [:cloudera ;; For hadoop-* *-*cdh4*
                           :platypope ;; For avro-mapred 1.7.5 hadoop1-cdh4
                           {:dependencies
                            [[org.apache.hadoop/hadoop-core "2.0.0-mr1-cdh4.1.2"]
                             [org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.2"]
                             [org.apache.avro/avro-mapred "1.7.5"
                              :classifier "hadoop1-cdh4"
                              :exclusions [org.apache.avro/avro-ipc]]]}]
             :hadoop-2-2-0 {:dependencies
                            [[org.apache.hadoop/hadoop-client "2.2.0"]
                             [org.apache.hadoop/hadoop-common "2.2.0"]
                             [org.apache.avro/avro-mapred "1.7.5"
                              :classifier "hadoop2"
                              :exclusions [org.apache.avro/avro-ipc]]]}})
