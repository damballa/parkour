(defproject parkour "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories ^:replace [["damballa" "http://jars.repo.rnd.atl.damballa/"]]
  :warn-on-reflection true
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [com.damballa/abracad "0.3.0"]
                 [com.damballa/avro-mapred "1.7.4.2"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "1.0.3"
                :exclusions [org.codehaus.jackson/jackson-mapper-asl]]]}})
