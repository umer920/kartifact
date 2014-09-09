package kgroup.kartifact;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


//What I need for kafka

/*
*    PRODUCER
*    broker list
*    topic (atleast name)
*    messages source 
*/

//for topic: host broker, number of partitions, replications factor, name 

/*
* CONSUMER
* zookeeper address
* groupID
* topic
* number of threads
*/

public class KafkaProducer extends Connector{

	@Override
	public boolean authenticate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean fetch(Map<String, Object> params) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean put(Map<String, Object> params) {
		long events = Long.parseLong(params.get("size").toString()); 
		String brokers = params.get("brokers").toString();
        
		Random rnd = new Random();
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kgroup.kartifact.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               System.out.println(msg);
               KeyedMessage<String, String> data = new KeyedMessage<String, String>(params.get("topic").toString(), ip, msg);
               producer.send(data);
        }
        producer.close();
    
		return true;
	}

	@Override
	public String getSample() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String search(String searchFilter) {
		// TODO Auto-generated method stub
		return null;
	}

}
