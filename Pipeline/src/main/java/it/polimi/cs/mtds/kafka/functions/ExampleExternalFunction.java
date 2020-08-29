package it.polimi.cs.mtds.kafka.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

public class ExampleExternalFunction implements BiFunction<String, AtomicReference<String>, String> {

	/**
	 * Return a string consisting in the same characters of the input string, shuffled
	 */
	@Override
	public String apply(String string,AtomicReference<String> state) {
		final char[] charsA = (string+state.get()).toCharArray();
		final List<Character> characterList = new ArrayList<>(charsA.length);
		for ( char c : charsA ) characterList.add(c);
		Collections.shuffle(characterList);
		for(int i=0; i<characterList.size(); i++) charsA[i] = characterList.get(i);
		return new String(charsA);
	}
}
