package wikipedia.option;

public enum OptionType {
    InputPath(new InputPathOption()),
    OutputPath(new OutputPahOption()),
    SearchTopic(new SearchTopicOption()),
    RestrictSearch(new RestrictSearchOption()),
    Date(new DateOption()),
    NoTopTen(new NoTopTenOption()),
    Help(new HelpOption());

    private IOption option;

    private OptionType(IOption opt) {
        option = opt;
    }

    public IOption getOption() {
        return option;
    }
}
