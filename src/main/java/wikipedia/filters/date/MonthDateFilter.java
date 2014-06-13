package wikipedia.filters.date;


import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

public class MonthDateFilter extends DatePathFilter{

    @Override
    public boolean accept(Path path) {
        try{
            DateTime fileDate = parseFileDateTime(path);
            if(fileDate.getYear() != getDateTime().getYear())
                return false;
            return fileDate.getMonthOfYear() == getDateTime().getMonthOfYear();
        }
        catch(UnsupportedOperationException e){
            LOG.warn("Could not parse following path :" + path.getName());
            return true;
        }
    }
}
