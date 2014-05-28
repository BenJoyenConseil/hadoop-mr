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
import wikipedia.utils.ASCIINormalizer;
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
				langagesSelection = getWordList(context, file);
			else if(file.getName().startsWith("page_names_to_skip"))
				subjectsToIgnore = getWordList(context, file);
		}
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] fileNameSplits = StringUtils.split(key.toString(), '-');
		String[] recordSplits = StringUtils.split(value.toString(), ' ');
        if(fileNameSplits.length < 2 || recordSplits.length < 4) {
            return;
        }

        CustomKey outputKey = buildCustomKeyFromRecord(fileNameSplits[1], recordSplits);

        if(isRecordToBeIgnored(outputKey) || !isRecordLangSelected(outputKey.getLang()))
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

	private void addNewKey(CustomKey newKey) {
		List<CustomKey> topTen;
        topTen = getCustomKeyTopTen(newKey);

		if(topTen.contains(newKey)){
            updateCount(newKey, topTen);
        }
		else if(topTen.size() < 10)
			topTen.add(newKey);
		else{
			CustomKey min = Collections.min(topTen, new CountComparator());
			if(min.getCount() < newKey.getCount()){
                replaceKey(topTen, newKey, min);
			}
		}
	}

    private CustomKey buildCustomKeyFromRecord(String fileNameSplits, String[] recordSplits) {
        CustomKey outputKey = new CustomKey();
        //lang
        String lang = recordSplits[0].toLowerCase();
        outputKey.setLang(lang);
        //date
        DateTime date = formatter.parseDateTime(fileNameSplits);
        outputKey.setDay(date.getDayOfMonth());
        outputKey.setMonth(date.getMonthOfYear());
        outputKey.setYear(date.getYear());
        //name
        outputKey.setPageName(ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(recordSplits[1])));
        //count
        long count = Long.parseLong(recordSplits[2]);
        outputKey.setCount(count);
        return outputKey;
    }

    boolean isRecordLangSelected(String recordLang) {
        return langagesSelection.contains(recordLang);
    }

    boolean isRecordToBeIgnored(CustomKey outputKey) {
        if(subjectsToIgnore.contains(outputKey.getPageName()))
            return true;
        for(String subject : subjectsToIgnore){
            if(outputKey.getPageName().contains(subject.toLowerCase()))
                return true;
        }
        return false;
    }

    private List<String> getWordList(Context context, Path file) throws IOException {
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

    private void replaceKey(List<CustomKey> topTen, CustomKey newKey, CustomKey oldKey) {
        topTen.remove(oldKey);
        topTen.add(newKey);
    }

    private void updateCount(CustomKey newKey, List<CustomKey> topTen) {
        for(CustomKey k : topTen)
            if(k == newKey)
                k.setCount(k.getCount() + newKey.getCount());
    }

    private List<CustomKey> getCustomKeyTopTen(CustomKey newKey) {
        List<CustomKey> topTen;
        if(!topTenByLang.containsKey(newKey.getLang()))
            topTenByLang.put(newKey.getLang(), new ArrayList<CustomKey>(10));

        topTen = topTenByLang.get(newKey.getLang());
        return topTen;
    }
}
