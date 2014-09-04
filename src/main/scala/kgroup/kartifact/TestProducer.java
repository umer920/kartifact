package kgroup.kartifact;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
    public static void main(String []args) {
    	// TODO Auto-generated method stub
		
    			Properties props = new Properties();
    			 
    			props.put("metadata.broker.list", "208.95.234.50:9092,208.95.234.54:9092");
    			props.put("serializer.class", "kafka.serializer.StringEncoder");
    			// props.put("partitioner.class", "example.producer.SimplePartitioner");
    			props.put("request.required.acks", "1");
    			 
    			ProducerConfig config = new ProducerConfig(props);
    			
    			Producer<String, String> producer = new Producer<String, String>(config);
    			
    			String date = "04092014" ;
    			// String topic = "my-replicated-topic" ;
    			String topic = "multi2" ;
    			
    			
    			for (int i = 1 ; i <= 1000 ; i++) {
    				
    				String msg = date + " This is message " + i ;
    				System.out.println(msg) ;
    				
    				KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(i), msg);
    				 
    				producer.send(data);
    				
    				
    			}
    			
    			producer.close();
    }
}
