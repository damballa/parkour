(defproject com.damballa/parkour "0.1.2"
  :description "Hadoop MapReduce in idiomatic Clojure."
  :url "http://github.com/damballa/parkour"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :warn-on-reflection true
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.damballa/abracad "0.4.2"]
                 [org.apache.avro/avro-mapred "1.7.5"
                  :exclusions [org.apache.avro/avro-ipc]]]
  :profiles {:provided
             {:dependencies
              [[org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
               [org.apache.hadoop/hadoop-core "1.2.1"
                :exclusions [org.codehaus.jackson/jackson-mapper-asl]]
               [log4j "1.2.17"]]}})
