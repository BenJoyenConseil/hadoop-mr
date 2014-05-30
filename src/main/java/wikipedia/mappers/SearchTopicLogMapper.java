package wikipedia.mappers;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import wikipedia.CustomKey;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class SearchTopicLogMapper extends LogMapper {
    private final int LEVENSHTEIN_THRESHOLD = 80;
    List<String> subjectsToFilter;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

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

            else if(getLevenshteinPourcent(outputKey.getPageName(), subject.toLowerCase()) > LEVENSHTEIN_THRESHOLD)
                return false;
        }
        return true;
    }

    float getLevenshteinPourcent(String str1, String str2){
        int distance = StringUtils.getLevenshteinDistance(str1, str2);
        int longerString = Math.max(str1.length(), str2.length());
        float i = (float)distance / (float)longerString;
        float result = (1 - i) * 100;
        return result;
    }
}
