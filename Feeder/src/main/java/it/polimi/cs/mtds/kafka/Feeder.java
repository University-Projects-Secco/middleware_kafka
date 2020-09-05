package it.polimi.cs.mtds.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static java.lang.Integer.parseInt;
import static java.lang.Thread.sleep;

public class Feeder {

	private static final DateTimeFormatter clock = DateTimeFormatter.ofPattern("HH:mm:ss");
	private static KafkaProducer<String,String> producer;
	private static final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	public static void main(String[] args) throws IOException {
		final Properties properties = new Properties();
		final String propFile = args.length>0?args[0]:"config.properties";
		final InputStreamReader propertiesIn = new InputStreamReader(new FileInputStream(propFile), StandardCharsets.UTF_8);
		try {
			properties.load(propertiesIn);
		}catch ( IOException e ){ throw new IOException("Cannot read property file",e); }
		properties.put("key.serializer", StringSerializer.class);
		properties.put("key.deserializer", StringDeserializer.class);
		properties.put("value.serializer", StringSerializer.class);
		properties.put("value.deserializer", StringDeserializer.class);
		producer = new KafkaProducer<>(properties);
		help();
		int key=0;
		String cmd;
		do{
			cmd=in.readLine();
			if("help".equalsIgnoreCase(cmd)||"h".equalsIgnoreCase(cmd)){
				help();
				continue;
			}
			try{
				if(cmd.startsWith("auto")){
					final String[] splitted = cmd.split("\\s|,|\\.|-|_");
					final int messages = parseInt(splitted[1]);
					final int bulkSize = splitted.length>=3? parseInt(splitted[2]):Integer.MAX_VALUE;
					for ( int i = 0,j=1; i < messages; i++, j++ ) {
						sendMessage(key++, key);
						if(j%bulkSize==0) {
							System.out.println("waiting 5 seconds");
							sleep(5000);
						}
					}
				}else {
					final int val = parseInt(cmd);
					sendMessage(key++,val);
				}
			}catch ( NumberFormatException | ArrayIndexOutOfBoundsException e ){
				System.out.println("Please insert integer numbers, 'auto %d', or 'end'");
			} catch ( InterruptedException e ) {
				e.printStackTrace();
			}
		}while ( !"end".equals(cmd) );
	}

	private static void sendMessage(int key, int value){
		final ProducerRecord<String, String> record = new ProducerRecord<>("topic_1", String.valueOf(key), String.valueOf(value));
		producer.send(record);
		System.out.println(clock.format(LocalDateTime.now()) + ": sent '" + value + "'. type 'end' to quit");
	}

	private static void help(){
		System.out.println("\n\nUsage:\n" +
				"%d sends the value %d\n" +
				"auto-%d1[-%d2] sends %d1 messages automatically generated, waiting 5 seconds every %d2 messages\n" +
				"end quits\n" +
				"help|h shows this message\n\n");
	}

}
