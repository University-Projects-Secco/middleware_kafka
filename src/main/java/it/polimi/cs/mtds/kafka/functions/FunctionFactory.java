package it.polimi.cs.mtds.kafka.functions;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public interface FunctionFactory<Input,State,Output> {
	BiFunction<Input, AtomicReference<State>,Output> getFunction(String functionName);
}
