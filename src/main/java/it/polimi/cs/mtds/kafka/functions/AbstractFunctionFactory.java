package it.polimi.cs.mtds.kafka.functions;

import java.util.function.Function;

public abstract class AbstractFunctionFactory<V> {

	private final static String STRING_CANONICAL_NAME = String.class.getCanonicalName();

	public abstract Function<V,V> getFunction(String functionName);
	public static <V> AbstractFunctionFactory<V> getInstance(Class<V> vClass){
		final String vClassCanonicalName = vClass.getCanonicalName();
		if(vClassCanonicalName.equals(STRING_CANONICAL_NAME)) return ( AbstractFunctionFactory<V> ) new StringFunctionFactory();
		else throw new UnsupportedOperationException("Unsupported value type");
	}
}
