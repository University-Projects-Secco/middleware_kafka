package it.polimi.cs.mtds.kafka.functions;

import java.util.function.Function;

public final class FunctionFactory {
	public static Function<String,String> getFunction(String name){
		switch ( name.toLowerCase() ){
			case "int:increment":
				return s->String.valueOf(Integer.parseInt(s)+1);
			case "int:decrement":
				return s->String.valueOf(Integer.parseInt(s)-1);
			case "int:double":
				return s->String.valueOf(Integer.parseInt(s)*2);
			case "int:half":
				return s->String.valueOf(Integer.parseInt(s)/2);
			case "string:print":
				return s->{
					System.out.println(s);
					return s;
				};
			default:
				return Function.identity();
		}
	}
}
