package it.polimi.cs.mtds.kafka.stage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public abstract class KafkaClient<Key,Input,State,Output> implements Runnable, Closeable {
	protected final KafkaConsumer<Key,Input> consumer;
	protected final KafkaProducer<Key,Output> producer;
	protected final AtomicReference<State> stateRef;
	private final String upstreamConsumerGroupId;   //TODO: wrong approach. reason more about how you update the offsets.

	protected KafkaClient(KafkaConsumer<Key, Input> consumer, KafkaProducer<Key, Output> producer, AtomicReference<State> stateRef, String upstreamConsumerGroupId) {
		this.consumer = consumer;
		this.producer = producer;
		this.stateRef = stateRef;
		this.upstreamConsumerGroupId = upstreamConsumerGroupId;
	}

	protected void updateOffsets(ConsumerRecords<Key,Input> records){
		//Prepare updated offsets
		final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

		//Update offsets: for each partition "used", set new offset = max offset + 1 (aka, all messages read)
		records.partitions().parallelStream().forEach(partition -> {
			final List<ConsumerRecord<Key, Input>> recordsForPartition = records.records(partition);
			final ConsumerRecord<Key, Input> lastRecord = recordsForPartition.get(recordsForPartition.size() - 1);
			final long offset = lastRecord.offset();
			offsets.put(partition, new OffsetAndMetadata(offset + 1));
		});

		//add offsets to transaction
		producer.sendOffsetsToTransaction(offsets, upstreamConsumerGroupId); //Consumers of the next stage
	}
}
