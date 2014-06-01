package wikipedia.options;


public class HelpOption implements IOption {

    private static final String HELP_OPTION_TAG = "-help";

    @Override
    public String getCode() {
        return HELP_OPTION_TAG;
    }
}
