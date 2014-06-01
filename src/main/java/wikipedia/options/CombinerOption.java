package wikipedia.options;

public class CombinerOption implements IOption{

    private static final String COMBINER_OPTION_TAG = "-useCombiner";

    @Override
    public String getCode() {
        return COMBINER_OPTION_TAG;
    }
}
