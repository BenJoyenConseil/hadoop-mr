package wikipedia.options;

import org.apache.hadoop.fs.Path;


public class SearchTopicOption extends AbstractOption<Path> {

    private static final String SEARCH_TOPIC_FILE_OPTION_TAG = "-searchTopicFile";

    public SearchTopicOption() {
        super(SEARCH_TOPIC_FILE_OPTION_TAG);
    }


    @Override
    protected void setValueFromArg(String arg) {
        setValue(new Path(arg));
    }
}
