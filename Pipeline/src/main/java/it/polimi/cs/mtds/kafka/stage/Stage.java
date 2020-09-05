package it.polimi.cs.mtds.kafka.stage;

import it.polimi.cs.mtds.kafka.stage.communication.ContentManager;
import it.polimi.cs.mtds.kafka.stage.communication.StateManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.logging.Logger;

public class Stage<Key,Input, State, Output> implements Runnable{

	private final ContentManager<Key,Input, State,Output> contentManager;

	private final StateManager<State> stateManager;

	private boolean running;

	public Stage(final BiFunction<Input,AtomicReference<State>,Output> function,
	             final State initialState,
	             final int stageNum,
	             final int parallelUnitId,
	             final String bootstrap_servers) throws IOException {

		AtomicReference<State> stateRef = new AtomicReference<>(initialState);

		this.stateManager = StateManager.build(stateRef, stageNum, parallelUnitId,bootstrap_servers);

		this.contentManager = ContentManager.build(function, stateRef, stageNum, parallelUnitId,bootstrap_servers);

		this.running = true;
	}

	@Override
	public void run() {
		try {
			while ( running ) {
				contentManager.run();
				stateManager.run();
				contentManager.commit();
				stateManager.commit();
			}
		}finally {
			contentManager.close();
			stateManager.close();
		}
	}

	public void shutdown(){
		running = false;
	}
}
