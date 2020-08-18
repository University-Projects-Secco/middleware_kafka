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
import java.util.stream.StreamSupport;

import static it.polimi.cs.mtds.kafka.constants.Constants.*;

class StateManager<State> extends KafkaClient<String,State,State,State> {

	private final String replicaId;

	public static <State> StateManager<State> build(final AtomicReference<State> stateRef,
	                                                final int stageNum,
	                                                final int parallelUnitId) throws IOException {
		final String replicaId = stageNum+"-"+parallelUnitId;
		final String stateManagerId = STATE_GROUP_PREFIX+replicaId;
		final String consumerGroupId = CONSUMER_GROUP_PREFIX + stateManagerId;
		final String producerTransactionalId = PRODUCER_GROUP_PREFIX + stateManagerId;

		//Configure consumer
		final Properties consumerProperties = new Properties();
		final InputStream consumerPropIn = Stage.class.getClassLoader().getResourceAsStream("state_consumer.properties");
		consumerProperties.load(consumerPropIn);
		consumerProperties.put(GROUP_ID, consumerGroupId);

		//Configure producer
		final Properties producerProperties = new Properties();
		final InputStream producerPropIn = Stage.class.getClassLoader().getResourceAsStream("state_producer.properties");
		producerProperties.load(producerPropIn);
		producerProperties.put(TRANSACTIONAL_ID,producerTransactionalId);

		return new StateManager<>(stateRef, replicaId, consumerGroupId, new KafkaConsumer<>(consumerProperties), new KafkaProducer<>(producerProperties));
	}

	private StateManager(final AtomicReference<State> stateRef,
	                     final String replicaId,
	                     final String upstreamConsumerGroupId,
	                     KafkaConsumer<String, State> consumer,
	                     KafkaProducer<String, State> producer) {
		super(consumer, producer, stateRef, upstreamConsumerGroupId);

		//Initialize trivial attributes and variables
		this.replicaId = replicaId;

		//initialize consumer
		consumer.subscribe(Collections.singleton(STATE_GROUP_PREFIX));

		//load pre-existing state, if any
		//ConsumerRecords<String,State> states = stateConsumer.poll(Duration.ofMillis(Long.parseLong(stateWriterProperties.getProperty("transaction.timeout.ms"))+2000));
		ConsumerRecords<String,State> states = consumer.poll(Duration.ofSeconds(2));
		StreamSupport.stream(states.spliterator(),false)
				.filter(record->replicaId.equals(record.key()))
				.forEach(record-> this.stateRef.set(record.value()));

		//initialize producer
		producer.initTransactions();
	}

	@Override
	public void run() {
		producer.beginTransaction();

		final ProducerRecord<String,State> newStateRecord= new ProducerRecord<>(STATE_GROUP_PREFIX, this.replicaId, this.stateRef.get());
		producer.send(newStateRecord);

		consumer.seekToBeginning(consumer.assignment());

//		final ConsumerRecords<String,State> records = consumer.poll(Duration.ofSeconds(5));
//
//		updateOffsets(records);

		consumer.commitSync();
		producer.commitTransaction();
	}

	@Override
	public void close() {
		consumer.close();
		producer.close();
	}
}
