package wikipedia.filters;


import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class WeekDatePathFilterTest {

    @Test
    public void accept_shouldReturnTrueWhenFilterWeek_IsEqualToWeek_OfDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140526");
        String dateString = "20140601";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new WeekDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void accept_shouldReturnFalseWhenFilterWeek_IsNotEqualToWeek_OfDateExtractedFromPath() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140526");
        String dateString = "20140525";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new WeekDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(false)));
    }

    @Test
    public void accept_shouldReturnFalseWhenFilterWeek_IsAnotherYearThanFile() throws Exception {
        // Given
        Path path = new Path("pagecounts-20140528");
        String dateString = "20130528";
        DatePathFilter.setDateTime(dateString);

        // When
        boolean result = new WeekDatePathFilter().accept(path);

        // Then
        assertThat(result, is(equalTo(false)));
    }
}
