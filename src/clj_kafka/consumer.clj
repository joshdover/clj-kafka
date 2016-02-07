(ns ^{:doc "Clojure interface for KafkaConsumer API. For complete Javadocs see:
  http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/package-summary.html"}
  clj-kafka.consumer
  (:import [java.util Iterator]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecords]
           [org.apache.kafka.common.serialization Deserializer ByteArrayDeserializer StringDeserializer])
  (:use [clj-kafka.core :only (to-clojure)]))

(defn- lazy-iterate
  [^java.util.Iterator it]
  (lazy-seq
    (when (.hasNext it)
      (cons (.next it) (lazy-iterate it)))))

(defn- records-seq
  [^ConsumerRecords records]
  (map to-clojure
    (lazy-iterate (.iterator ^Iterable records))))

(defn- messages-for-topic
  [msgs topic]
  (records-seq (.records msgs topic)))

(defn default-deserializer
  []
  (StringDeserializer.))

(defn consumer
  ([^java.util.Map configs]
    (KafkaConsumer. configs (default-deserializer) (default-deserializer)))
  ([^java.util.Map configs ^Deserializer key-deserializer ^Deserializer value-deserializer]
    (KafkaConsumer. configs key-deserializer value-deserializer)))

(defn messages
  ([consumer topic]
    (messages consumer topic 10000))
  ([consumer topic timeout]
    (let [msgs (.poll consumer timeout)]
      ; (println (str "Retrieved " (count msgs) " messages"))
      (messages-for-topic msgs topic))))

(defn shutdown
  [^KafkaConsumer consumer]
  (.wakeup consumer))
