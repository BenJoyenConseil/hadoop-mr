package wikipedia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<CustomKey, LongWritable, CustomKey, LongWritable> {

	private LongWritable outputValue;
	//private List<CustomKey> top;
	private Map<String, List<CustomKey>> topTenByLang;
	private final int topMapSize = 10;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		outputValue = new LongWritable(0);
		//top = new ArrayList<CustomKey>(10);
		topTenByLang = new HashMap<String, List<CustomKey>>();
	}
	
	@Override
	protected void reduce(CustomKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

		long total = 0;
		for(LongWritable count : values){
			total += count.get();
		}
		key.setCount(total);
		addKeyValueToTopMap((CustomKey)key.clone());
		
	}

	private void addKeyValueToTopMap(CustomKey key) {
		if(!topTenByLang.containsKey(key.getLang()))
			topTenByLang.put(key.getLang(), new ArrayList<CustomKey>(topMapSize));
		
		List<CustomKey> top = topTenByLang.get(key.getLang());
		
		if(top.size() < topMapSize){
			top.add(key);
		}
		else{
			CustomKey min = Collections.min(top, new CountComparator());
			if(min.getCount() < key.getCount()){
				top.remove(min);
				top.add(key);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context)	throws IOException, InterruptedException {

		for(String lang : topTenByLang.keySet()){
			List<CustomKey> top = topTenByLang.get(lang);
			SortedSet<CustomKey> sortedTop = new TreeSet<CustomKey>(new Comparator<CustomKey>() {
				@Override
				public int compare(CustomKey arg0, CustomKey arg1) {
					return (int)(arg1.getCount() - arg0.getCount());
				}
			});
			sortedTop.addAll(top);
			for(CustomKey k : sortedTop){
				outputValue.set(k.getCount());
				context.write(k, outputValue);
			}
		}
	}
	
}
