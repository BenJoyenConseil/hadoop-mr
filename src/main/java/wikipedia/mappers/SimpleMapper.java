package wikipedia.mappers;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import wikipedia.domain.PageNameAndCount;
import wikipedia.utils.ASCIINormalizer;
import wikipedia.utils.UTF8Decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


public class SimpleMapper extends Mapper<LongWritable, Text, Text, PageNameAndCount> {

    private List<String> restrictions = new ArrayList<String>();
    private String[] langs = {"en", "fr", "de", "es"};
    private Text outputKey = new Text();
    private PageNameAndCount outputValue = new PageNameAndCount();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FSDataInputStream stream;
        BufferedReader reader;
        URI uri = context.getCacheFiles()[0];
        Path file = new Path(uri);
        stream = FileSystem.get(context.getConfiguration()).open(file);
        reader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            restrictions.add(line.toLowerCase());
        }
        reader.close();
        stream.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] recordSplits = StringUtils.split(value.toString(), ' ');
        if (recordSplits.length < 4) {
            return;
        }

        for(String lang : langs){
            if(!lang.equalsIgnoreCase(recordSplits[0]))
                continue;
            String pageName = ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(recordSplits[1]));
            if(isRecordToBeIgnored(pageName))
                break;


            long count = Long.parseLong(recordSplits[2]);
            outputKey.set(lang);
            outputValue.setPageName(pageName);
            outputValue.setCount(count);
            context.write(outputKey, outputValue);
        }
    }

    public boolean isRecordToBeIgnored(String pageName) {
        for (String subject : restrictions) {
            if (pageName.contains(subject.toLowerCase()))
                return true;
        }
        return false;
    }
}
