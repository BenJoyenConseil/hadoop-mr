package wikipedia.filters;


import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

public class WeekDatePathFilter extends DatePathFilter{

    @Override
    public boolean accept(Path path) {
        DateTime fileDate = getFileDateTime(path);
        if(fileDate.getYear() != getDateTime().getYear())
            return false;
        return fileDate.getWeekOfWeekyear() == getDateTime().getWeekOfWeekyear();
    }
}
