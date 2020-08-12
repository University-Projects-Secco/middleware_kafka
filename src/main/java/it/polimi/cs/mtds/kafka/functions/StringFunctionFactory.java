package it.polimi.cs.mtds.kafka.functions;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public final class StringFunctionFactory implements FunctionFactory<String,Integer,String> {

	/**
	 * Builds the predefined functions. Lambda can be replaced with custom functional interfaces ({@link ExampleExternalFunction})
	 * @param name an identifier for the function
	 * @return a predefined function identified by that name
	 */
	public BiFunction<String, AtomicReference<Integer>, String> getFunction(String name){
		switch ( name.toLowerCase() ){
			case "int:increment":
				return (string,state)->String.valueOf(Integer.parseInt(string)+1);
			case "int:decrement":
				return (string,state)->String.valueOf(Integer.parseInt(string)-1);
			case "int:double":
				return (string,state)->String.valueOf(Integer.parseInt(string)*2);
			case "int:half":
				return (string,state)->String.valueOf(Integer.parseInt(string)/2);
			case "string:print":
				return (string,state)->{
					System.out.println(state.getAndAccumulate(1,Integer::sum)+" "+string);
					return string;
				};
			case "string:shuffle":
				return new ExampleExternalFunction();
			default:
				return (string,state)->string;
		}
	}
}
