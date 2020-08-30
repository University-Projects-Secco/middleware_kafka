#!/bin/bash
bash ../../kafka_2.12-2.3.1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic states
for i in {0..4} ; do
    bash ../../kafka_2.12-2.3.1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "topic_$i"
done