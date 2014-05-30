package wikipedia.option;


import org.apache.hadoop.fs.Path;

public class InputPathOption extends AbstractOption<Path>{

    private static final String INPUT_PATH_OPTION_TAG = "-in";
    private static final String DEFAULT_INPUTPATH = "wikipedia";

    public InputPathOption() {
        super(INPUT_PATH_OPTION_TAG);
    }

    public InputPathOption(String[] args) {
        super(INPUT_PATH_OPTION_TAG, args);
    }

    @Override
    protected void setValueFromArg(String arg) {
        setValue(new Path(arg));
    }

    @Override
    public void setValueFromArgs(String[] args) {
        super.setValueFromArgs(args);
        if(value == null){
            setValueFromArg(DEFAULT_INPUTPATH);
        }
    }
}
