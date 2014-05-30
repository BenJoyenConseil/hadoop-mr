package wikipedia.filters.date;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

public class DayDatePathFilter extends DatePathFilter{

    @Override
    public boolean accept(Path path) {
        try {
            DateTime fileDate = parseFileDateTime(path);
            if (fileDate.getYear() != getDateTime().getYear())
                return false;
            if(fileDate.getDayOfYear() != getDateTime().getDayOfYear())
                return false;
            return fileDate.getWeekOfWeekyear() == getDateTime().getWeekOfWeekyear();
        } catch (UnsupportedOperationException e) {
            LOG.error("Could not parse following path :" + path.getName());
            return true;
        }
    }
}
