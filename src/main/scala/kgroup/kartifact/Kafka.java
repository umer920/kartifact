package kgroup.kartifact;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.admin.CreateTopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Kafka extends Connector {
	
	
	private ConsumerConnector consumer;
    private String topic;
    private  ExecutorService executor;
 
    public void setKafkaConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        while(true){
        	//for (final KafkaStream stream : streams) {
        		executor.execute(new ConsumerTest(streams.get(0), threadNumber,new KafkaReceiver()));
        		//threadNumber++;
        	//}
        }
    }
 
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "999999");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
    
    public Producer<String, String> getProducer(String brokers)
	{
		//String brokers = params.get("brokers").toString();
		
		Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kgroup.kartifact.SimplePartitioner");
        props.put("request.required.acks", "1");
        
        ProducerConfig config = new ProducerConfig(props);
        
        Producer<String, String> producer = new Producer<String, String>(config);
        
        return producer;
        
	}

	@Override
	public boolean authenticate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object fetch(Map<String, Object> params) {
		
		String zooKeeper = params.get("zk").toString(); 
        String groupId = "1";
        String topic = params.get("topic").toString();
        
        setKafkaConsumer(zooKeeper,groupId,topic);
        
        int threads = 1;
        
        run(threads);
 
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
 
        }
        shutdown();
		return null;
	}

	@Override
	public boolean put(Map<String, Object> params) {
		
		String [] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "cloud2:2181";
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "1";
        arguments[6] = "--topic";
        arguments[7] = params.get("topic").toString();

        CreateTopicCommand.main(arguments);
		
		
		Producer<String, String> producer = getProducer(params.get("brokers").toString());
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(params.get("topic").toString(), "null", params.get("message").toString());
		System.out.println();
		producer.send(data);
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
