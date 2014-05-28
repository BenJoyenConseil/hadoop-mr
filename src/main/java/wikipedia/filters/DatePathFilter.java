package wikipedia.filters;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DatePathFilter implements PathFilter {

    private static DateTime dateTime;
    private static String datePattern = "yyyyMMdd";
    protected static DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);


    @Override
    public boolean accept(Path path) {
        DateTime fileDate = getFileDateTime(path);
        return fileDate.equals(dateTime);
    }

    protected DateTime getFileDateTime(Path path) {
        String[] fileNameSplits = StringUtils.split(path.getName(), '-');
        return formatter.parseDateTime(fileNameSplits[1]);
    }

    public static DateTime getDateTime() {
        return dateTime;
    }

    public static void setDateTime(String dateString) {
        DatePathFilter.dateTime = formatter.parseDateTime(dateString);
    }
}
