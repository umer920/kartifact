package kgroup.kartifact;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner<String> {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
    
    @Override
    public int partition(String key, int a_numPartitions) {
        int partition = 0;
        int offset = key.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( key.substring(offset+1)) % a_numPartitions;
        }
       return partition;
    }

	/*@Override
	public int partition(Object arg0, int arg1) {
		// TODO Auto-generated method stub
		return 1;
	}*/
 
}
