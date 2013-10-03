(defproject com.damballa/parkour "0.1.9-SNAPSHOT"
  :description "Hadoop MapReduce in idiomatic Clojure."
  :url "http://github.com/damballa/parkour"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :warn-on-reflection true
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.damballa/abracad "0.4.5"]
                 [org.apache.avro/avro "1.7.5"]
                 [org.apache.avro/avro-mapred "1.7.5"
                  :exclusions [org.apache.avro/avro-ipc]]]
  :aliases {"test" ["with-profile" ~(str ;;"default,hadoop-0-20-2:"
                                         ;;"default,hadoop-0-20-205:"
                                         "default,hadoop-1-0-3:"
                                         "default,hadoop-1-2-1:"
                                         "default,hadoop-cdh3:"
                                         "default,hadoop-cdh4")
                    ,              "test"]}
  :profiles {:provided {:dependencies
                        [[org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                         [org.apache.hadoop/hadoop-core "1.2.1"]
                         [org.slf4j/slf4j-api "1.6.1"]
                         [org.slf4j/slf4j-log4j12 "1.6.1"]
                         [log4j "1.2.17"]]}
             :test {:resource-paths ["test-resources"]}
             :hadoop-0-20-2 {:dependencies
                             [[org.apache.hadoop/hadoop-core "0.20.2"]]}
             :hadoop-0-20-205 {:dependencies
                               [[org.apache.hadoop/hadoop-core "0.20.205.0"]]}
             :hadoop-1-0-3 {:dependencies
                            [[org.apache.hadoop/hadoop-core "1.0.3"]]}
             :hadoop-1-2-1 {:dependencies
                            [[org.apache.hadoop/hadoop-core "1.2.1"]]}
             :hadoop-cdh3 {:repositories
                           [["cloudera" ~(str "https://repository.cloudera.com"
                                              "/artifactory/cloudera-repos/")]]
                           :dependencies
                           [[org.apache.hadoop/hadoop-core "0.20.2-cdh3u3"]]}
             :hadoop-cdh4 {:repositories
                           [["cloudera" ~(str "https://repository.cloudera.com"
                                              "/artifactory/cloudera-repos/")]
                            ["damballa" "http://jars.repo.rnd.atl.damballa/"]]
                           :dependencies
                           [[org.apache.hadoop/hadoop-core "2.0.0-mr1-cdh4.1.2"]
                            [org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.2"]
                            [org.apache.avro/avro-mapred "1.7.5"
                             :classifier "hadoop1-cdh4"
                             :exclusions [org.apache.avro/avro-ipc]]]}})
