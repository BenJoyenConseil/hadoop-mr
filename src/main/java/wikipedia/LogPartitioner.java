package wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import wikipedia.domain.CustomKey;

public class LogPartitioner extends Partitioner<CustomKey, LongWritable>{

	@Override
	public int getPartition(CustomKey key, LongWritable value, int numPartitions) {
		return (key.getLang().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
