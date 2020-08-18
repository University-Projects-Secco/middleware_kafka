package it.polimi.cs.mtds.kafka.constants;

import org.apache.kafka.common.protocol.types.Field;

public final class Constants {
	private Constants(){}
	public static final String GROUP_ID = "group.id";
	public static final String TRANSACTIONAL_ID = "transactional.id";
	public static final String TOPIC_PREFIX = "topic_";
	public static final String CONSUMER_GROUP_PREFIX = "consumer-";
	public static final String PRODUCER_GROUP_PREFIX = "producer-";
	public static final String STATE_GROUP_PREFIX = "states-";
}
