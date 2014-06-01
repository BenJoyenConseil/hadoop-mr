package wikipedia.options;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

public class DateOption extends AbstractOption<DateTime> {

    private static final String DATE_OPTION_TAG = "-date";

    public DateOption() {
        super(DATE_OPTION_TAG);
    }

    @Override
    protected void setValueFromArg(String arg) {
            setValue(DateTime.parse(arg, DateTimeFormat.forPattern("yyyyMMdd")));
    }
}
