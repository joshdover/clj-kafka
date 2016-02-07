(defproject org.clojars.joshdover/clj-kafka "0.5.0"
  :min-lein-version "2.0.0"
  :url "https://github.com/joshdover/clj-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-clients "0.9.0.0"]]
  :plugins [[lein-expectations "0.0.8"]
            [lein-codox "0.9.1"]]
  :codox {:source-uri "https://github.com/pingles/clj-kafka/blob/{version}/{filepath}#L{line}"
          :metadata {:doc/format :markdown}}
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [[org.apache.kafka/kafka_2.11 "0.9.0.0"]
                                  [commons-io/commons-io "2.4"]
                                  [expectations "2.1.1"]]}}
  :description "Clojure wrapper for Kafka's Java API")
