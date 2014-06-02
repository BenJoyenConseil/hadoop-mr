package wikipedia.mappers;

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
import wikipedia.CountComparator;
import wikipedia.domain.CustomKey;
import wikipedia.utils.ASCIINormalizer;
import wikipedia.utils.UTF8Decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class LogMapper extends Mapper<Text, Text, CustomKey, LongWritable> {

	private static DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
    static List<String> langagesSelection;
    private Map<String, List<CustomKey>> topTenByLang;
    private static boolean topTenMapper;

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {

		topTenByLang = new HashMap<String, List<CustomKey>>();

        if (langagesSelection != null)
            return;
		for(URI uri : context.getCacheFiles()){
			Path file = new Path(uri);
			if(file.getName().startsWith("languages_selection"))
				langagesSelection = getWordList(context, file);
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

        if(isTopTenMapper())
		    addNewKeyInTopTen(outputKey);
        else
            context.write(outputKey, new LongWritable(outputKey.getCount()));
	}

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(!isTopTenMapper())
            return;

        for(String lang : topTenByLang.keySet()){
            List<CustomKey> topTen = topTenByLang.get(lang);
            for(CustomKey k : topTen){
                context.write(k, new LongWritable(k.getCount()));
            }
        }
    }

	protected void addNewKeyInTopTen(CustomKey newKey) {
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

    public CustomKey buildCustomKeyFromRecord(String fileName, String[] recordSplits) {
        CustomKey outputKey = new CustomKey();
        //lang
        String lang = recordSplits[0].toLowerCase();
        outputKey.setLang(lang);
        //date
        DateTime date = formatter.parseDateTime(fileName);
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
        return false;
    }

    protected List<String> getWordList(Context context, Path file) throws IOException {
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

    public static boolean isTopTenMapper() {
        return topTenMapper;
    }

    public static void setTopTenMapper(boolean topTenMapper) {
        LogMapper.topTenMapper = topTenMapper;
    }
}
