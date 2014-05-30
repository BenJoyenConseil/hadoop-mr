package wikipedia.option;


public class OptionCollection {
    private String[] args;

    public OptionCollection(String[] args){
        this.args = args;
        new OptionCollection(args).contains(OptionType.Date);
    }

    public boolean contains(OptionType optionType){
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase(optionType.getOption().getCode()))
                return true;
        }
        return false;
    }

    public AbstractOption get(OptionType optionType){
        switch (optionType){
            case InputPath:
                return new InputPathOption(args);
            case OutputPath:
                return new OutputPahOption(args);
            case SearchTopic:
                SearchTopicOption option = new SearchTopicOption();
                option.setValueFromArgs(args);
                return option;
            case RestrictSearch:
                RestrictSearchOption restrictSearchOption = new RestrictSearchOption();
                restrictSearchOption.setValueFromArgs(args);
                return restrictSearchOption;
            case Date:
                DateOption dateOption = new DateOption();
                dateOption.setValueFromArgs(args);
                return dateOption;
            case Help:
                throw new UnsupportedOperationException("Cannot return Help");
            default:
                throw new UnsupportedOperationException("Option unknown");
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for(OptionType o : OptionType.values()){
            builder.append(o.getOption().getCode());
            builder.append('\n');
        }
        return builder.toString();
    }
}

