package it.polimi.cs.mtds.kafka.stage;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static it.polimi.cs.mtds.kafka.constants.Constants.*;
import static org.apache.kafka.common.protocol.CommonFields.GROUP_ID;
import static org.apache.kafka.common.protocol.CommonFields.TRANSACTIONAL_ID;

class ContentManager<Key,Input, State, Output> extends KafkaClient<Key,Input,State,Output> {

	private final BiFunction<Input, AtomicReference<State>, Output> function;
	private final String outputTopic;

	public static <Key, Input, State, Output> ContentManager<Key,Input,State,Output> build(final BiFunction<Input, AtomicReference<State>, Output> function,
	                                                                                       final AtomicReference<State> stateRef,
	                                                                                       final int stageNum) throws IOException{
		//Configure consumer
		final Properties consumerProperties = new Properties();
		final InputStream consumerPropIn = Stage.class.getClassLoader().getResourceAsStream("consumer.properties");
		consumerProperties.load(consumerPropIn);
		consumerProperties.put(GROUP_ID.name,CONSUMER_GROUP_PREFIX+stageNum);

		//Configure producer
		final Properties producerProperties = new Properties();
		final InputStream producerPropIn = Stage.class.getClassLoader().getResourceAsStream("producer.properties");
		producerProperties.load(producerPropIn);
		producerProperties.put(TRANSACTIONAL_ID.name,PRODUCER_GROUP_PREFIX+(stageNum+1));

		final String upstreamConsumerGroupId = CONSUMER_GROUP_PREFIX + (stageNum+1);

		return new ContentManager<>(function,stateRef,stageNum,upstreamConsumerGroupId,new KafkaConsumer<>(consumerProperties),new KafkaProducer<>(producerProperties));
	}

	private ContentManager(final BiFunction<Input, AtomicReference<State>, Output> function,
	                       final AtomicReference<State> stateRef,
	                       final int stageNum,
	                       final String upstreamConsumerGroupId,
	                       final KafkaConsumer<Key,Input> consumer,
	                       final KafkaProducer<Key,Output> producer) {
		super(consumer,producer, stateRef, upstreamConsumerGroupId);

		//Initialize trivial attributes
		this.function = function;
		this.outputTopic = TOPIC_PREFIX+(stageNum+1);

		//initialize producers and consumers
		producer.initTransactions();
		consumer.subscribe(Collections.singleton(TOPIC_PREFIX+stageNum));
	}

	@Override
	public void run() {
		producer.beginTransaction();

		//Get some messages from the previous stage
		final ConsumerRecords<Key, Input> records = consumer.poll(Duration.ofSeconds(20));

		//If some message is present work with them
		if(records.count()>0) {

			//Execute the function on each message and send the result to the upstream group using the same key
			records.forEach(record -> {
				final Key key = record.key();
				final Output result = function.apply(record.value(), this.stateRef);
				final ProducerRecord<Key, Output> resultRecord = new ProducerRecord<>(outputTopic, key, result);
				producer.send(resultRecord);
			});

			updateOffsets(records);
		}

		consumer.commitSync();
		producer.commitTransaction();
	}

	public void close(){
		consumer.close();
		producer.close();
	}
}
