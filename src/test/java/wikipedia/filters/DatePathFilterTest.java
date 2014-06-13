package wikipedia.filters;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.junit.Test;
import wikipedia.filters.date.DatePathFilter;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DatePathFilterTest {

    @Test
    public void accept_shouldReturnTrueWhenFilterDate_IsEqualToDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140528");
        String dateString = "20140528";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new DatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void accept_shouldReturnFalseWhenFilterDate_IsNotEqualToDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140528");
        String dateString = "20140527";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new DatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(false)));
    }

    @Test
    public void parseFileDateTime(){
        // Given
        Path path = new Path("pagecounts-20140510-230000.txt");
        String dateString = "20140510";
        DatePathFilter.setDateTime(dateString);
        DateTime expected = new DateTime(2014, 5, 10, 0, 0);

        // When
        DateTime result = new DatePathFilter().parseFileDateTime(path);

        // Then
        assertThat(result, is(equalTo(expected)));
    }

    List<Path> buildOneDayOfWikipediaLogFileList(){
        List<Path> list = new ArrayList<Path>(24);
        String s = "pagecounts-20140510-%s0000.txt";

        for (int i = 0; i < 24; i++){
            if(i < 10) {
                list.add(new Path(String.format(s, "0" + i)));
            }
            else {
                list.add(new Path(String.format(s, "" + i)));
            }
        }

        return list;
    }
}
