package wikipedia.options;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class DateOption extends AbstractOption<DateTime> {

    private static final String DATE_OPTION_TAG = "-date";

    public DateOption() {
        super(DATE_OPTION_TAG);
    }

    public DateOption(String[] args){
        super(DATE_OPTION_TAG, args);

        if(!this.contains(args))
            throw new RuntimeException("L'option -date n'existe pas. Veuillez la préciser...");
    }

    @Override
    protected void setValueFromArg(String arg) {
            setValue(DateTime.parse(arg, DateTimeFormat.forPattern("yyyyMMdd")));
    }
}
