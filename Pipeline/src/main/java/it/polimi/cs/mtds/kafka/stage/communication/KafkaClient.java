package it.polimi.cs.mtds.kafka.stage.communication;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class KafkaClient<Key,Input,State,Output> implements Runnable, Closeable {
	protected final KafkaConsumer<Key,Input> consumer;
	protected final KafkaProducer<Key,Output> producer;
	protected final AtomicReference<State> stateRef;
	private final String consumerGroupId;

	protected KafkaClient(KafkaConsumer<Key, Input> consumer, KafkaProducer<Key, Output> producer, AtomicReference<State> stateRef, String consumerGroupId) {
		this.consumer = consumer;
		this.producer = producer;
		this.stateRef = stateRef;
		this.consumerGroupId = consumerGroupId;
	}

	protected void updateOffsets(ConsumerRecords<Key,Input> records){
		final Map<TopicPartition,OffsetAndMetadata> offsets = records.partitions().parallelStream()
				.collect(Collectors.toMap(Function.identity(),
						partition->{
							final List<ConsumerRecord<Key, Input>> recordsForPartition = records.records(partition);
							final ConsumerRecord<Key, Input> lastRecord = recordsForPartition.get(recordsForPartition.size() - 1);
							return new OffsetAndMetadata(lastRecord.offset()+1);
						}));

		//add offsets to transaction
		producer.sendOffsetsToTransaction(offsets, consumerGroupId); //Consumers of the next stage
	}

	public void commit(){
		producer.commitTransaction();
	}

}
