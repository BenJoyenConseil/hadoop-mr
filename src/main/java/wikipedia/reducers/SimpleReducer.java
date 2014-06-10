package wikipedia.reducers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import wikipedia.domain.PageNameAndCount;

import java.io.IOException;
import java.util.*;


public class SimpleReducer extends Reducer<Text, PageNameAndCount, Text, PageNameAndCount> {

    private Comparator<PageNameAndCount> descComparator;
    private Comparator<PageNameAndCount> ascComparator;
    private final int numberResultByLang = 100;
    Map<String, TreeSet<PageNameAndCount>> topTenByLang;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        descComparator = new Comparator<PageNameAndCount>() {
            @Override
            public int compare(PageNameAndCount o1, PageNameAndCount o2) {
                return o2.getCount().compareTo(o1.getCount());
            }
        };
        ascComparator = new Comparator<PageNameAndCount>() {
            @Override
            public int compare(PageNameAndCount o1, PageNameAndCount o2) {
                return o1.getCount().compareTo(o2.getCount());
            }
        };
        topTenByLang = new HashMap<String, TreeSet<PageNameAndCount>>();
    }

    @Override
    protected void reduce(Text key, Iterable<PageNameAndCount> values, Context context) throws IOException, InterruptedException {

        for(PageNameAndCount item : values){
            String lang = new String(key.toString());
            addKeyValueToTopMap(lang, item.clone());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String lang : topTenByLang.keySet()){
            TreeSet<PageNameAndCount> top = topTenByLang.get(lang);

            for(PageNameAndCount pageNameAndCount : top.descendingSet()){
                context.write(new Text(lang), pageNameAndCount);
            }
        }
    }

    void addKeyValueToTopMap(String lang, PageNameAndCount value) {
        if(!topTenByLang.containsKey(lang)){
            topTenByLang.put(lang, new TreeSet<PageNameAndCount>());
        }

        TreeSet<PageNameAndCount> top = topTenByLang.get(lang);
        PageNameAndCount tmp = retrievePageNameAndCount(value.getPageName(), top);
        if(tmp != null){
            value.setCount(value.getCount() + tmp.getCount());
            top.remove(tmp);
            top.add(value);
        }

        if(top.size() < numberResultByLang)
            top.add(value);
        else{
            PageNameAndCount min = top.first();
            if(min.getCount() < value.getCount()){
                top.remove(min);
                top.add(value);
            }
        }
    }

    PageNameAndCount retrievePageNameAndCount(String pageName, TreeSet<PageNameAndCount> set) {
        for (PageNameAndCount pageNameAndCount : set) {
            if (pageNameAndCount.getPageName().equals(pageName)) {
                return pageNameAndCount;
            }
        }

        return null;
    }
}
