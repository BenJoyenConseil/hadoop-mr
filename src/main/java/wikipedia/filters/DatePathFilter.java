package wikipedia.filters;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatePathFilter implements PathFilter {

	protected static Logger LOG = LoggerFactory.getLogger(DatePathFilter.class);
    private static DateTime dateTime;
    private static String datePattern = "yyyyMMdd";
    protected static DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);


    @Override
    public boolean accept(Path path) {
    	try{
    		DateTime fileDate = parseFileDateTime(path);
            return fileDate.equals(dateTime);
    	}
    	catch(UnsupportedOperationException e){
        	LOG.error("Could not parse following path :" + path.getName());
        	return true;
    	}
    }

    protected DateTime parseFileDateTime(Path path) throws UnsupportedOperationException {
        String[] fileNameSplits = StringUtils.split(path.getName(), '-');
        String dateString;

        if(fileNameSplits.length <= 1)
        	throw new UnsupportedOperationException("not wikipedia log file");
        
        dateString = fileNameSplits[1];
        return formatter.parseDateTime(dateString);
    }

    public static DateTime getDateTime() {
        return dateTime;
    }

    public static void setDateTime(String dateString) {
        DatePathFilter.dateTime = formatter.parseDateTime(dateString);
    }
}
