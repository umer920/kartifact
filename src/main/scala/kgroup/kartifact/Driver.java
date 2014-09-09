package kgroup.kartifact;

import java.util.HashMap;
import java.util.Map;

public class Driver {

	public static void main(String[] args) {
		if(args.length == 3)
		{
			Connector producer = new KafkaProducer();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("topic", args[0]);
			params.put("brokers", args[1]);
			params.put("size", args[2]);
			producer.put(params);
			producer.setSource(args[1]);
		}
		else
		{
			//public KafkaConsumer(String a_zookeeper, String a_groupId, String a_topic)
			Connector consumer = new KafkaConsumer(args[0], args[1], args[2]);
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("zk", args[0]);
			params.put("group", args[1]);
			params.put("topic", args[2]);
			params.put("threads", args[3]);
			consumer.setSource(args[0]+","+args[2]);
		}

	}

}
