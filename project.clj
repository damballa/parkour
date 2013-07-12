(defproject parkour "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :warn-on-reflection true
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "1.0.3"]]}})
