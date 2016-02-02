(ns clj-kafka.test.consumer
  (:use [expectations]
        [clj-kafka.core :only (with-resource to-clojure)]
        [clj-kafka.producer :only (producer send record)]
        [clj-kafka.test.utils :only (with-test-broker)])
  (:require [clj-kafka.consumer :as consumer]))

(def producer-config {"metadata.broker.list" "localhost:9999"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "test"})

(def consumer-config {"zookeeper.connect" "localhost:2182"
                      "group.id" "clj-kafka.test.consumer"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "false"})

(defn test-record
  []
  (record "test" (.getBytes "Hello, world")))

(defn send-and-receive
  [messages]
  (with-test-broker test-broker-config
    (with-resource [c (consumer/consumer consumer-config)]
      consumer/shutdown
      (let [p (producer producer-config)]
        (send p messages)
        (first (consumer/messages c "test"))))))

(expect {:topic "test"
         :offset 0
         :partition 0} (in (send-and-receive [(test-record)])))

; (given (send-and-receive [(test-record)])
;        (expect :topic "test"
;                :offset 0
;                :partition 0
;                (string-value :value) "Hello, world"))
