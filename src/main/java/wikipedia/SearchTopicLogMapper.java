package wikipedia;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

public class SearchTopicLogMapper extends LogMapper {
    List<String> subjectsToFilter;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        FSDataInputStream stream;
        BufferedReader reader;
        for (URI uri : context.getCacheFiles()) {
            Path file = new Path(uri);
            if (file.getName().startsWith("search_topic_dictionary"))
                subjectsToFilter = getWordList(context, file);
        }
    }

    @Override
    boolean isRecordToBeIgnored(CustomKey outputKey) {
        if (subjectsToFilter.contains(outputKey.getPageName()))
            return false;
        for (String subject : subjectsToFilter) {
            if (outputKey.getPageName().contains(subject.toLowerCase()))
                return false;

            else if(getLevenshteinIndex(outputKey.getPageName(), subject.toLowerCase()) > 80)
                return false;
        }
        return true;
    }

    float getLevenshteinIndex(String str1, String str2){
        int distance = StringUtils.getLevenshteinDistance(str1, str2);
        int longerString = Math.max(str1.length(), str2.length());
        float i = (float)distance / (float)longerString;
        float result = (1 - i) * 100;
        return result;
    }
}
