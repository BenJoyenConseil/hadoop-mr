package wikipedia;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import wikipedia.utils.UTF8Decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class LogMapper extends Mapper<Text, Text, CustomKey, LongWritable> {

	private DateTimeFormatter formatter;
	private Map<String, List<CustomKey>> topTenByLang;
	List<String> langagesSelection;
	List<String> subjectsToIgnore;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		formatter = DateTimeFormat.forPattern("yyyyMMdd");
		topTenByLang = new HashMap<String, List<CustomKey>>();
		
		FSDataInputStream stream;
		BufferedReader reader;
		for(URI uri : context.getCacheFiles()){
			Path file = new Path(uri);
			if(file.getName().startsWith("languages_selection"))
				langagesSelection = fillTheList(context, file);
			else if(file.getName().startsWith("page_names_to_skip"))
				subjectsToIgnore = fillTheList(context, file);
		}
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] fileNameSplit = StringUtils.split(key.toString(), '-');
		String[] values = StringUtils.split(value.toString(), ' ');
		if(fileNameSplit.length < 2 || values.length < 4)
			return;

		CustomKey outputKey = new CustomKey();

		DateTime date = formatter.parseDateTime(fileNameSplit[1]);
		outputKey.setDay(date.getDayOfMonth());
		outputKey.setMonth(date.getMonthOfYear());
		outputKey.setYear(date.getYear());
		outputKey.setLang(values[0].toLowerCase());
		outputKey.setPageName(UTF8Decoder.unescape(values[1]));
		long count = Long.parseLong(values[2].toLowerCase());
		outputKey.setCount(count);

		if(!isRecordLangSelected(outputKey) || isRecordToBeIgnored(outputKey))
			return;

		addNewKey(outputKey);
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String lang : topTenByLang.keySet()){
            List<CustomKey> topTen = topTenByLang.get(lang);
            for(CustomKey k : topTen){
                context.write(k, new LongWritable(k.getCount()));
            }
        }
    }

    private List<String> fillTheList(Context context, Path file) throws IOException {
        List<String> referenceList = new ArrayList<String>();
        FSDataInputStream stream;
        BufferedReader reader;
        stream = FileSystem.get(context.getConfiguration()).open(file);
        reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        referenceList = new ArrayList<String>();
        while((line = reader.readLine()) != null){
            referenceList.add(line.toLowerCase());
        }
        reader.close();
        stream.close();
        return referenceList;
    }

	void addNewKey(CustomKey newKey) {
		List<CustomKey> topTen;
		if(!topTenByLang.containsKey(newKey.getLang()))
			topTenByLang.put(newKey.getLang(), new ArrayList<CustomKey>(10));

		topTen = topTenByLang.get(newKey.getLang());

		if(topTen.contains(newKey)){
			for(CustomKey k : topTen)
				if(k == newKey)
					k.setCount(k.getCount() + newKey.getCount());
		}
		else if(topTen.size() < 10)
			topTen.add(newKey);
		else{
			CustomKey min = Collections.min(topTen, new CountComparator());
			if(min.getCount() < newKey.getCount()){
				topTen.remove(min);
				topTen.add(newKey);
			}
		}
	}

    boolean isRecordLangSelected(CustomKey outputKey) {
        return langagesSelection.contains(outputKey.getLang());
    }

    boolean isRecordToBeIgnored(CustomKey outputKey) {
        if(subjectsToIgnore.contains(outputKey.getPageName()))
            return true;
        for(String subject : subjectsToIgnore){
            if(subject.contains(outputKey.getPageName()))
                return true;
        }
        return false;
    }
}
