package wikipedia.filters;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import wikipedia.filters.date.DatePathFilter;

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
}
