package it.polimi.cs.mtds.kafka.stage.communication;

import it.polimi.cs.mtds.kafka.stage.Stage;
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

public class ContentManager<Key,Input, State, Output> extends KafkaClient<Key,Input,State,Output> {

	private final BiFunction<Input, AtomicReference<State>, Output> function;
	private final String outputTopic;

	public static <Key, Input, State, Output> ContentManager<Key,Input,State,Output> build(final BiFunction<Input, AtomicReference<State>, Output> function,
	                                                                                       final AtomicReference<State> stateRef,
	                                                                                       final int stageNum,
	                                                                                       final String bootstrap_servers) throws IOException{
		final String consumerGroupId = CONSUMER_GROUP_PREFIX+stageNum;

		//Configure consumer
		final Properties consumerProperties = new Properties();
		final InputStream consumerPropIn = Stage.class.getClassLoader().getResourceAsStream("consumer.properties");
		consumerProperties.load(consumerPropIn);
		consumerProperties.put(GROUP_ID,consumerGroupId);
		consumerProperties.put("bootstrap.servers",bootstrap_servers);

		//Configure producer
		final Properties producerProperties = new Properties();
		final InputStream producerPropIn = Stage.class.getClassLoader().getResourceAsStream("producer.properties");
		producerProperties.load(producerPropIn);
		producerProperties.put(TRANSACTIONAL_ID,PRODUCER_GROUP_PREFIX+(stageNum+1));
		producerProperties.put("bootstrap.servers",bootstrap_servers);

		return new ContentManager<>(function,stateRef,stageNum, consumerGroupId, new KafkaConsumer<>(consumerProperties),new KafkaProducer<>(producerProperties));
	}

	private ContentManager(final BiFunction<Input, AtomicReference<State>, Output> function,
	                       final AtomicReference<State> stateRef,
	                       final int stageNum,
	                       final String consumerGroupId,
	                       final KafkaConsumer<Key,Input> consumer,
	                       final KafkaProducer<Key,Output> producer) {
		super(consumer,producer, stateRef, consumerGroupId);

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
	}

	public void close(){
		consumer.close();
		producer.close();
	}
}
