package wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogPartitioner extends Partitioner<CustomKey, LongWritable>{
	

	@Override
	public int getPartition(CustomKey key, LongWritable value, int numPartitions) {
		return Math.abs(key.getLang().hashCode() % numPartitions);
	}
}
