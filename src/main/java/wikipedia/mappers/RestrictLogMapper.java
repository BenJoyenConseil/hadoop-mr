package wikipedia.mappers;


import org.apache.hadoop.fs.Path;
import wikipedia.CustomKey;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class RestrictLogMapper extends LogMapper{
    List<String> subjectsToIgnore;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        for (URI uri : context.getCacheFiles()) {
            Path file = new Path(uri);
            if (file.getName().startsWith("page_names_to_skip"))
                subjectsToIgnore = getWordList(context, file);
        }
    }

    @Override
    boolean isRecordToBeIgnored(CustomKey outputKey) {
        if (subjectsToIgnore.contains(outputKey.getPageName()))
            return true;
        for (String subject : subjectsToIgnore) {
            if (outputKey.getPageName().contains(subject.toLowerCase()))
                return true;
        }
        return false;
    }
}
