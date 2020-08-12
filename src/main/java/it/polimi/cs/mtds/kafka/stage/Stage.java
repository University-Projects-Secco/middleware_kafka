package it.polimi.cs.mtds.kafka.stage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class Stage<Key, Value, State> implements Runnable{
	private static final String TOPIC_PREFIX = "topic_";
	private static final String CONSUMER_GROUP_PREFIX = "consumer-";
	private static final String PRODUCER_GROUP_PREFIX = "producer-";

	private final BiFunction<Value, AtomicReference<State>, Value> function;
	private final KafkaConsumer<Key, Value> consumer;
	private final KafkaProducer<Key, Value> producer;
	private volatile boolean running;
	private final String upstreamConsumerGroupId;
	private final String outputTopic;
	private final AtomicReference<State> stateRef;

	public Stage(final BiFunction<Value,AtomicReference<State>,Value> function, State initialState, final int stageNum) throws IOException {
		//Configure consumer
		final Properties consumerProperties = new Properties();
		final InputStream consumerPropIn = Stage.class.getClassLoader().getResourceAsStream("consumer.properties");
		consumerProperties.load(consumerPropIn);
		consumerProperties.put("group.id",CONSUMER_GROUP_PREFIX+stageNum);

		//Configure producer
		final Properties producerProperties = new Properties();
		final InputStream producerPropIn = Stage.class.getClassLoader().getResourceAsStream("producer.properties");
		producerProperties.load(producerPropIn);
		producerProperties.put("transactional.id",PRODUCER_GROUP_PREFIX+(stageNum+1));

		this.function = function;
		this.stateRef = new AtomicReference<>(initialState);
		this.upstreamConsumerGroupId = CONSUMER_GROUP_PREFIX + (stageNum + 1);
		this.outputTopic = TOPIC_PREFIX+(stageNum+1);
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.producer = new KafkaProducer<>(producerProperties);
		producer.initTransactions();
		consumer.subscribe(Collections.singleton(TOPIC_PREFIX+stageNum));
		this.running = true;
	}

	@Override
	public void run() {
		try {
			while ( running ) {

				producer.beginTransaction();

				try {
					//Get some messages from the previous stage
					final ConsumerRecords<Key, Value> records = consumer.poll(Duration.ofMinutes(1));

					//Apply the stage function to the messages and send the results
					this.executeFunctionAndSendResult(records);

					//Update the offsets for each partition
					this.updateOffsets(records);

					//TODO: save state

					consumer.commitSync();
					producer.commitTransaction();
				}catch ( Exception e ){
					e.printStackTrace();
					producer.abortTransaction();
				}
			}
		}finally {
			consumer.close();
			producer.close();
		}
	}

	/**
	 * For each of the inputs, apply the stage's function and send the result.
	 * The target partition is automatically decided by kafka, evaluating hash(key)%partitionNumber
	 * @param records the ConsumerRecords collection of inputs
	 */
	private void executeFunctionAndSendResult(ConsumerRecords<Key,Value> records){
		records.forEach(record->{
			final Key key = record.key();
			final Value result = function.apply(record.value(), this.stateRef);
			final ProducerRecord<Key, Value> resultRecord = new ProducerRecord<>(outputTopic, key, result);
			producer.send(resultRecord);
		});
	}

	private void updateOffsets(ConsumerRecords<Key,Value> records){
		final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

		//Update offsets: increase by 1 the last offset of each partition
		records.partitions().parallelStream().forEach(partition->{
			final List<ConsumerRecord<Key, Value>> recordsForPartition = records.records(partition);
			final ConsumerRecord<Key,Value> lastRecord = recordsForPartition.get(recordsForPartition.size()-1);
			final long offset = lastRecord.offset();
			offsets.put(partition, new OffsetAndMetadata(offset + 1));
		});

		//add offsets to transaction
		producer.sendOffsetsToTransaction(offsets, upstreamConsumerGroupId); //Consumers of the next stage
	}

	public void shutdown(){
		running = false;
	}
}
