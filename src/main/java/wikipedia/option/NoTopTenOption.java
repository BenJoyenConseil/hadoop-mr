package wikipedia.option;


public class NoTopTenOption implements IOption{
    private static final String NO_TOP_TEN_OPTION_TAG = "-noTopTen";

    @Override
    public String getCode() {
        return NO_TOP_TEN_OPTION_TAG;
    }
}
