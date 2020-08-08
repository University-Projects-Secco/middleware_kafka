package it.polimi.cs.mtds.kafka;

import it.polimi.cs.mtds.kafka.functions.FunctionFactory;
import it.polimi.cs.mtds.kafka.stage.Stage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {

	private static final List<Thread> stages = new LinkedList<>();

	public static void main(String[] args) throws IOException, InterruptedException {
		try {
			Properties processProperties = new Properties();
			InputStream propertiesIn = Main.class.getClassLoader().getResourceAsStream("config.properties");
			processProperties.load(propertiesIn);
			final List<Integer> stages = Arrays.stream((( String ) processProperties.get("stages")).split(","))
					.map(Integer::parseInt).collect(Collectors.toUnmodifiableList());
			final String[] functions = ((String) processProperties.get("functions")).split(",");
			if(stages.size()!=functions.length){
				System.err.println("Invalid property file: the same number of stages and functions is required");
				return;
			}
			for(int i=0; i<functions.length; i++){
				final Thread stage = new Thread(new Stage(functions[i],stages.get(i),new Properties(),new Properties()),"Stage "+i); //URG: supply properties for producer and consumer
				Main.stages.add(stage);
				stage.start();
			}
			System.out.println("All stages running");
		} catch ( IOException e ) {
			throw new IOException("Cannot read property file",e);
		}finally {
			for ( Thread stage : stages ) stage.join();
		}
	}
}
