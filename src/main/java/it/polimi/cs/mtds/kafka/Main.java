package it.polimi.cs.mtds.kafka;

import it.polimi.cs.mtds.kafka.stage.Stage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

	private static final List<Thread> stageThreads = new LinkedList<>();
	private static final List<Stage<Integer,String>> stages = new LinkedList<>();
	private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

	/**
	 * Open config.properties
	 * Read the list of stages and function executed at each stage from properties
	 * Start a new thread for each stage ({@link Stage})
	 * Join all stages
	 *
	 * @throws IOException if fails to open config.properties
	 */
	public static void main(String[] args) throws IOException {

		//Prepare properties
		final Properties processProperties = new Properties();
		final InputStream propertiesIn = Main.class.getClassLoader().getResourceAsStream("config.properties");
		try {
			processProperties.load(propertiesIn);
		}catch ( IOException e ){ throw new IOException("Cannot read property file",e); }
		final Properties producerProperties = new Properties();
		final Properties consumerProperties = new Properties();
		final String bootstrapServers = processProperties.getProperty(BOOTSTRAP_SERVERS);

		//Initialize common properties
		producerProperties.put(BOOTSTRAP_SERVERS,bootstrapServers);
		producerProperties.put("enable.idempotence","true");
		consumerProperties.put(BOOTSTRAP_SERVERS,bootstrapServers);
		consumerProperties.put("enable.auto.commit","false");
		consumerProperties.put("isolation.level","read_committed");

		//Read list of stages on this process
		final List<Integer> stages = Arrays.stream(processProperties.getProperty("stages").split(","))
				.map(Integer::parseInt).collect(Collectors.toUnmodifiableList());

		//Read function names for each stage
		final String[] functions = processProperties.getProperty("functions").split(",");

		//Safety check
		if(stages.size()!=functions.length) throw new IllegalStateException("Invalid property file: the same number of stages and functions is required");

		//Start the stages
		for(int i=0; i<functions.length; i++){
			final Stage<Integer,String> stage = new Stage<>(functions[i],String.class,stages.get(i),consumerProperties,producerProperties);
			final Thread stageThread = new Thread(stage,"Stage "+i);
			Main.stageThreads.add(stageThread);
			Main.stages.add(stage);
			stageThread.start();
		}

		//Handle SIGINT
		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			Main.stages.forEach(Stage::shutdown);
			for ( Thread thread : Main.stageThreads )
				try { thread.join(); } catch ( InterruptedException e ) {
					e.printStackTrace();
					System.err.println("ShutdownHook interrupted?");
					return;
				}
			System.out.println("All stages closed");
		}));

		//Output correct start info
		System.out.println("All stages running");
	}
}
