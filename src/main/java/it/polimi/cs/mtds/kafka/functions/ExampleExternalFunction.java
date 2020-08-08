package it.polimi.cs.mtds.kafka.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ExampleExternalFunction implements Function<String, String> {

	@Override
	public String apply(String s) {
		final char[] charsA = s.toCharArray();
		final List<Character> characterList = new ArrayList<>(charsA.length);
		for ( char c : charsA ) characterList.add(c);
		Collections.shuffle(characterList);
		for(int i=0; i<characterList.size(); i++) charsA[i] = characterList.get(i);
		return new String(charsA);
	}
}
