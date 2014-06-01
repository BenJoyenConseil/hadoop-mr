package wikipedia.options;


import org.apache.hadoop.fs.Path;

public class RestrictSearchOption extends AbstractOption<Path>{
    private static final String RESTRICTED_SEARCH_OPTION_TAG = "-restrictionFile";

    public RestrictSearchOption() {
        super(RESTRICTED_SEARCH_OPTION_TAG);
    }

    @Override
    protected void setValueFromArg(String arg) {
        setValue(new Path(arg));
    }
}
