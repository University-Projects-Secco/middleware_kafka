package it.polimi.cs.mtds.kafka.stage;

import it.polimi.cs.mtds.kafka.functions.FunctionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;

public class Stage implements Runnable{
	final Function<String, String> function;
	final int stageNumber;
	final KafkaConsumer<String, String> consumer;
	final KafkaProducer<String, String> producer;
	final Collection<String> inputTopics;
	final String outputTopic;
	private volatile boolean running;

	public Stage(String functionName, int stageNum, Properties consumerProperties, Properties producerProperties){
		this.function = FunctionFactory.getFunction(functionName);
		this.stageNumber = stageNum;
		this.inputTopics = Collections.singleton("topic"+stageNum);
		this.outputTopic = "topic"+(stageNum+1);
		this.consumer = new KafkaConsumer<>(consumerProperties);
		this.producer = new KafkaProducer<>(producerProperties);
		this.running = true;
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(inputTopics);
			while ( running ) {
				//DO STUFF
				final ConsumerRecords<String,String> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES).toMillis());
				for ( final ConsumerRecord<String, String> record: records ){
					final String key = record.key();
					final String result  = function.apply(record.value());
					final ProducerRecord<String,String> resultRecord = new ProducerRecord<>(outputTopic,key,result); //OPT: specify partition?
					final Future<RecordMetadata> future = producer.send(resultRecord);
				}
			}
		}finally {
			consumer.close();
			producer.close();
		}
	}

	public void shutdown(){
		running = false;
	}
}
