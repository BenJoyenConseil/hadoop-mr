package wikipedia.options;

public enum OptionType {
    InputPath(new InputPathOption()),
    OutputPath(new OutputPahOption()),
    SearchTopic(new SearchTopicOption()),
    RestrictSearch(new RestrictSearchOption()),
    Date(new DateOption()),
    NoTopTen(new NoTopTenOption()),
    UseCombiner(new CombinerOption()),
    Help(new HelpOption());

    private IOption option;

    private OptionType(IOption opt) {
        option = opt;
    }

    public IOption getOption() {
        return option;
    }
}
