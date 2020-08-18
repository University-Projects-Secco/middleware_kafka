./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic states \
--config "cleanup.policy=compact" \
--config "delete.retention.ms=100" \
--config "segment.ms=100" \
--config "min.cleanable.dirty.ratio=0.01"