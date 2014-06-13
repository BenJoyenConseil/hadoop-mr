package wikipedia.filters;


import org.apache.hadoop.fs.Path;
import org.junit.Test;
import wikipedia.filters.date.DatePathFilter;
import wikipedia.filters.date.DayDatePathFilter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DayDatePathFilterTest {

    @Test
    public void accept_shouldReturnTrue_WhenFilteredDay_IsEqualToDayOfDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140601");
        String dateString = "20140601";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new DayDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void accept_shouldReturnFalse_WhenFilteredDay_IsNotEqualToDayOfDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140526");
        String dateString = "20140525";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new DayDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(false)));
    }

    @Test
    public void accept_shouldReturnFalse_WhenFilteredDay_IsAnotherYearThanInFileName() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140528");
        String dateString = "20130528";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new DayDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(false)));
    }
}
