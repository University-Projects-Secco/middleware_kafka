package it.polimi.cs.mtds.kafka.stage;

import it.polimi.cs.mtds.kafka.functions.AbstractFunctionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;

public class Stage<K,V> implements Runnable{
	private static final String TOPIC_PREFIX = "topic_";
	final Function<V,V> function;
	final int stageNumber;
	final KafkaConsumer<K, V> consumer;
	final KafkaProducer<K, V> producer;
	final Collection<String> inputTopics;
	final String outputTopic;
	static final String CONSUMER_GROUP_PREFIX = "consumer-";
	static final String PRODUCER_GROUP_PREFIX = "producer-";
	private volatile boolean running;

	public Stage(final String functionName, final Class<V> vClass, final int stageNum) throws IOException {
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

		this.function = AbstractFunctionFactory.getInstance(vClass).getFunction(functionName);
		this.stageNumber = stageNum;
		this.inputTopics = Collections.singleton(TOPIC_PREFIX+stageNum);
		this.outputTopic = TOPIC_PREFIX+(stageNum+1);
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.producer = new KafkaProducer<>(producerProperties);
		producer.initTransactions();
		consumer.subscribe(inputTopics);
		this.running = true;
	}

	@Override
	public void run() {
		try {
			while ( running ) {

				producer.beginTransaction();

				try {
					//Get some messages from the previous stage
					final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMinutes(5).toMillis());

					//Apply the stage function to the messages and send the results
					this.executeFunctionAndSendResult(records);

					//Update the offsets for each partition
					this.updateOffsets(records);

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
	 * For each of the inputs, apply the stage's function and send the result
	 * @param records the ConsumerRecords collection of inputs
	 */
	private void executeFunctionAndSendResult(ConsumerRecords<K,V> records){
		records.forEach(record->{
			final K key = record.key();
			final V result = function.apply(record.value());
			final ProducerRecord<K, V> resultRecord = new ProducerRecord<>(outputTopic, key, result);
			producer.send(resultRecord);
		});
	}

	private void updateOffsets(ConsumerRecords<K,V> records){
		final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

		//Update offsets: increase by 1 the last offset of each partition
		records.partitions().parallelStream().forEach(partition->{
			final List<ConsumerRecord<K, V>> recordsForPartition = records.records(partition);
			final ConsumerRecord<K,V> lastRecord = recordsForPartition.get(recordsForPartition.size()-1);
			final long offset = lastRecord.offset();
			offsets.put(partition, new OffsetAndMetadata(offset + 1));
		});

		//add offsets to transaction
		producer.sendOffsetsToTransaction(offsets, CONSUMER_GROUP_PREFIX + (stageNumber + 1)); //Consumers of the next stage
	}

	public void shutdown(){
		running = false;
	}
}
