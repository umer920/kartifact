package kgroup.kartifact;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerTest implements Runnable {
	KafkaReceiver receiver;
    private KafkaStream<byte[], byte[]> m_stream;
    private int m_threadNumber;
 
    public ConsumerTest(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber,KafkaReceiver receiver) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.receiver = receiver;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
        {
        	//receiver.doSomething(it.next().message().toString());
            System.out.println(new String(it.next().message()));
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
