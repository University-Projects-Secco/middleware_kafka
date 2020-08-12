package it.polimi.cs.mtds.kafka.stage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Stage<Key,Input, State, Output> implements Runnable{
	private static final String GROUP_ID = "group.id";
	private static final String TRANSACTIONAL_ID = "transactional.id";

	private static final String TOPIC_PREFIX = "topic_";
	private static final String CONSUMER_GROUP_PREFIX = "consumer-";
	private static final String PRODUCER_GROUP_PREFIX = "producer-";
	private static final String STATE_GROUP_PREFIX = "states";

	private final BiFunction<Input, AtomicReference<State>, Output> function;
	private final KafkaConsumer<Key, Input> consumer;
	private final KafkaProducer<Key, Output> producer;
	private final KafkaConsumer<String,State> stateConsumer;
	private final KafkaProducer<String, State> stateProducer;
	private volatile boolean running;
	private final String upstreamConsumerGroupId;
	private final String outputTopic;
	private final AtomicReference<State> stateRef;
	private final String replicaId;

	public Stage(final BiFunction<Input,AtomicReference<State>,Output> function, State initialState, final int stageNum, final int parallelUnitId) throws IOException {

		this.function = function;
		this.upstreamConsumerGroupId = CONSUMER_GROUP_PREFIX + (stageNum + 1);
		this.outputTopic = TOPIC_PREFIX+(stageNum+1);
		this.replicaId = stageNum+"-"+parallelUnitId;
		this.stateRef = new AtomicReference<>(initialState);
		final String stateOperationsId = STATE_GROUP_PREFIX+"-"+replicaId;

		//Configure consumer
		final Properties consumerProperties = new Properties();
		final InputStream consumerPropIn = Stage.class.getClassLoader().getResourceAsStream("consumer.properties");
		consumerProperties.load(consumerPropIn);
		consumerProperties.put(GROUP_ID,CONSUMER_GROUP_PREFIX+stageNum);  //TODO: manage partitions?
		final Properties stateLoaderProperties = ( Properties ) consumerProperties.clone();
		stateLoaderProperties.put(GROUP_ID,stateOperationsId);

		//Configure producer
		final Properties producerProperties = new Properties();
		final InputStream producerPropIn = Stage.class.getClassLoader().getResourceAsStream("producer.properties");
		producerProperties.load(producerPropIn);
		producerProperties.put(TRANSACTIONAL_ID,PRODUCER_GROUP_PREFIX+(stageNum+1));
		final Properties stateWriterProperties = ( Properties ) producerProperties.clone();
		stateWriterProperties.put(TRANSACTIONAL_ID,stateOperationsId);


		//create producers and consumers for data
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.producer = new KafkaProducer<>(producerProperties);

		//create producers and consumers for state
		this.stateConsumer = new KafkaConsumer<>(stateLoaderProperties);
		this.stateProducer = new KafkaProducer<>(producerProperties);

		stateConsumer.subscribe(Collections.singleton(stateOperationsId));
		ConsumerRecords<String,State> states = stateConsumer.poll(Duration.ofMillis(Long.parseLong(stateWriterProperties.getProperty("transaction.timeout.ms"))+5000));
		StreamSupport.stream(states.spliterator(),true)
				.filter(record->replicaId.equals(record.key()))
				.findFirst()
				.ifPresent(record->this.stateRef.set(record.value()));

		//initialize producers and consumers
		producer.initTransactions();
		stateProducer.initTransactions();
		consumer.subscribe(Collections.singleton(TOPIC_PREFIX+stageNum));

		this.running = true;
	}

	@Override
	public void run() {
		try {
			while ( running ) {

				producer.beginTransaction();
				stateProducer.beginTransaction();

				try {
					//Get some messages from the previous stage
					final ConsumerRecords<Key, Input> records = consumer.poll(Duration.ofMinutes(1));

					//Apply the stage function to the messages and send the results
					this.executeFunctionAndSendResult(records);

					//Update the offsets for each partition
					this.updateOffsets(records);

					//Save new state
					this.saveNewState();

					consumer.commitSync();
					producer.commitTransaction();
				}catch ( Exception e ){
					e.printStackTrace();
					producer.abortTransaction();
					stateProducer.abortTransaction();
				}
			}
		}finally {
			consumer.close();
			producer.close();
			stateProducer.close();
		}
	}

	/**
	 * For each of the inputs, apply the stage's function and send the result.
	 * The target partition is automatically decided by kafka, evaluating hash(key)%partitionNumber
	 * @param records the ConsumerRecords collection of inputs
	 */
	private void executeFunctionAndSendResult(ConsumerRecords<Key,Input> records){
		records.forEach(record->{
			final Key key = record.key();
			final Output result = function.apply(record.value(), this.stateRef);
			final ProducerRecord<Key, Output> resultRecord = new ProducerRecord<>(outputTopic, key, result);
			producer.send(resultRecord);
		});
	}

	private void updateOffsets(ConsumerRecords<Key,Input> records){
		final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

		//Update offsets: increase by 1 the last offset of each partition
		records.partitions().parallelStream().forEach(partition->{
			final List<ConsumerRecord<Key, Input>> recordsForPartition = records.records(partition);
			final ConsumerRecord<Key,Input> lastRecord = recordsForPartition.get(recordsForPartition.size()-1);
			final long offset = lastRecord.offset();
			offsets.put(partition, new OffsetAndMetadata(offset + 1));
		});

		//add offsets to transaction
		producer.sendOffsetsToTransaction(offsets, upstreamConsumerGroupId); //Consumers of the next stage
	}

	private void saveNewState(){
		final ProducerRecord<String,State> newStateRecord= new ProducerRecord<>(STATE_GROUP_PREFIX, this.replicaId, this.stateRef.get());
		stateProducer.send(newStateRecord, (recordMetadata, exception) -> {
			if(exception!=null) {
				final Collection<TopicPartition> consumerPartitions = consumer.assignment();
				consumer.seekToEnd(consumerPartitions);
				consumer.commitSync();
				stateProducer.commitTransaction();
			}else stateProducer.abortTransaction();
		});
	}

	public void shutdown(){
		running = false;
	}
}
