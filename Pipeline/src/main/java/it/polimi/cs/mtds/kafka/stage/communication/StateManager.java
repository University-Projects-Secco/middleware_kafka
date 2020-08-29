package it.polimi.cs.mtds.kafka.stage.communication;

import it.polimi.cs.mtds.kafka.stage.Stage;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.StreamSupport;

import static it.polimi.cs.mtds.kafka.constants.Constants.*;

public class StateManager<State> extends KafkaClient<String,State,State,State> {

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

		return new StateManager<>(stateRef,
				replicaId,
				consumerGroupId,
				new KafkaConsumer<>(consumerProperties),
				new KafkaProducer<>(producerProperties),
				Long.parseLong(producerProperties.getProperty(TRANSACTION_TIMEOUT_MS)));
	}

	private StateManager(final AtomicReference<State> stateRef,
	                     final String replicaId,
	                     final String consumerGroupId,
	                     final KafkaConsumer<String, State> consumer,
	                     final KafkaProducer<String, State> producer,
	                     final long transaction_timeout_ms) {
		super(consumer, producer, stateRef, consumerGroupId);

		//Initialize trivial attributes and variables
		this.replicaId = replicaId;

		//initialize consumer
		final Collection<TopicPartition> statePartitions = Collections.singleton(new TopicPartition(STATE_TOPIC,0));
		consumer.assign(statePartitions);
		consumer.seekToBeginning(statePartitions);

		//load pre-existing state, if any
		ConsumerRecords<String,State> states = consumer.poll(Duration.ofMillis(transaction_timeout_ms+2000));
		StreamSupport.stream(states.spliterator(),false)
				.filter(record->replicaId.equals(record.key()))
				.forEach(record-> this.stateRef.set(record.value()));

		//initialize producer
		producer.initTransactions();
	}

	@Override
	public void run() {
		producer.beginTransaction();

		final ProducerRecord<String,State> newStateRecord= new ProducerRecord<>(STATE_TOPIC, this.replicaId, this.stateRef.get());
		producer.send(newStateRecord);
	}

	@Override
	public void close() {
		consumer.close();
		producer.close();
	}
}
