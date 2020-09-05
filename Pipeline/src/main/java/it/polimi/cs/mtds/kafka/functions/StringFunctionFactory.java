package it.polimi.cs.mtds.kafka.functions;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.logging.Logger;

public final class StringFunctionFactory implements FunctionFactory<String,String,String> {
	
	private static final Logger logger = Logger.getLogger(StringFunctionFactory.class.getName());
	
	private static String getOperationInfo(String string, String state, String result, String operation){
		return "Executed "+operation+" on ("+string+","+state+") with result "+result;
	}

	/**
	 * Builds the predefined functions. Lambda can be replaced with custom functional interfaces ({@link ExampleExternalFunction})
	 * @param name an identifier for the function
	 * @return a predefined function identified by that name
	 */
	public BiFunction<String, AtomicReference<String>, String> getFunction(String name){
		switch ( name.toLowerCase() ){
			case "int:increment":
				return (string,state)->{
					final String result = String.valueOf(Integer.parseInt(string)+1);
					logger.info(getOperationInfo(string,state.get(),result, name.toLowerCase()));
					return result;
				};
			case "int:decrement":
				return (string,state)->{
					final String result = String.valueOf(Integer.parseInt(string)-1);
					logger.info(getOperationInfo(string,state.get(),result, name.toLowerCase()));
					return result;
				};
			case "int:double":
				return (string,state)->{
					final String result = String.valueOf(Integer.parseInt(string)*2);
					logger.info(getOperationInfo(string,state.get(),result, name.toLowerCase()));
					return result;
				};
			case "int:half":
				return (string,state)->{
					final String result = String.valueOf(Integer.parseInt(string)/2);
					logger.info(getOperationInfo(string,state.get(),result, name.toLowerCase()));
					return result;
				};
			case "int:add_state":
				return (string, state)->{
					final String result = String.valueOf(Integer.parseInt(string)+Integer.parseInt(state.getAndAccumulate("1",(st,inc)-> String.valueOf(Integer.parseInt(st)+Integer.parseInt(inc)))+" "+string));
					logger.info(getOperationInfo(string,state.get(),result, name.toLowerCase()));
					return result;
				};
			case "string:print":
				return (string,state)->{
					System.out.println(state.getAndAccumulate("1",(st,inc)-> String.valueOf(Integer.parseInt(st)+Integer.parseInt(inc)))+" "+string);
					return string;
				};
			case "string:shuffle":
				return new ExampleExternalFunction();
			default:
				return (string,state)->{
					logger.info(getOperationInfo(string,state.get(),string, name.toLowerCase()));
					return string;
				};
		}
	}
}
