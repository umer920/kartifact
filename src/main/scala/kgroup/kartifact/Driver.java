package kgroup.kartifact;

import java.util.HashMap;
import java.util.Map;

import kafka.javaapi.producer.Producer;

public class Driver {

	public static void main(String[] args) {
		Connector connect = new Kafka();
		Map<String, Object> params = new HashMap<String, Object>();
		if(args.length == 3)
		{
			params.put("topic", args[0]);
			params.put("brokers", args[1]);
			params.put("message", args[2]);
			connect.put(params);
		}
		else
		{
			params.put("zk", args[0]);
			params.put("group", args[1]);
			params.put("topic", args[2]);
			params.put("threads", args[3]);
			connect.fetch(params);
		}

	}

}
