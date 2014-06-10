package wikipedia.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import wikipedia.AscCountComparator;
import wikipedia.domain.CustomKey;

import java.io.IOException;
import java.util.*;

public class LogReducer extends Reducer<CustomKey, LongWritable, CustomKey, LongWritable> {

	private LongWritable outputValue;
	private Map<String, List<CustomKey>> topTenByLang;
	private final int topMapSize = 10;

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		outputValue = new LongWritable(0);
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

	@Override
	protected void cleanup(Context context)	throws IOException, InterruptedException {

        Comparator<CustomKey> comparator = new Comparator<CustomKey>() {
            @Override
            public int compare(CustomKey arg0, CustomKey arg1) {
                return (int) (arg1.getCount() - arg0.getCount());
            }
        };

		for(String lang : topTenByLang.keySet()){
			List<CustomKey> top = topTenByLang.get(lang);
            SortedSet<CustomKey> sortedTop = new TreeSet<CustomKey>(comparator);
			sortedTop.addAll(top);
			for(CustomKey k : sortedTop){
				outputValue.set(k.getCount());
				context.write(k, outputValue);
			}
		}
	}

    private void addKeyValueToTopMap(CustomKey key) {
        if(!topTenByLang.containsKey(key.getLang()))
            topTenByLang.put(key.getLang(), new ArrayList<CustomKey>(topMapSize));

        List<CustomKey> top = topTenByLang.get(key.getLang());

        if(top.size() < topMapSize){
            top.add(key);
        }
        else{
            CustomKey min = Collections.min(top, new AscCountComparator());
            if(min.getCount() < key.getCount()){
                top.remove(min);
                top.add(key);
            }
        }
    }

}
