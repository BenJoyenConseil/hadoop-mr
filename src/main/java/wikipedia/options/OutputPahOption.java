package wikipedia.options;


import org.apache.hadoop.fs.Path;

public class OutputPahOption extends AbstractOption<Path>{
    private static final String OUTPUT_PATH_OPTION_TAG = "-out";
    private static final String DEFAULT_OUTPUTPATH = "wikipedia-out";

    public OutputPahOption(String[] args) {
        super(OUTPUT_PATH_OPTION_TAG, args);
    }

    public OutputPahOption() {
        super(OUTPUT_PATH_OPTION_TAG);
    }

    @Override
    protected void setValueFromArg(String arg) {
        setValue(new Path(arg));
    }

    @Override
    public void setValueFromArgs(String[] args) {
        super.setValueFromArgs(args);
        if (value == null) {
            setValueFromArg(DEFAULT_OUTPUTPATH);
        }
    }
}
