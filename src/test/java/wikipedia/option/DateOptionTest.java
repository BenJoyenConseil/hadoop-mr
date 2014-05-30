package wikipedia.option;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.Is.is;

public class DateOptionTest {

    @Test
    public void setValueFromArgs_shouldSetValueDateTimeFromArgs() throws Exception {
        // Given
        DateOption option = new DateOption();
        String [] args = new String[2];
        args[0] = "-date";
        args[1] = "20130228";
        DateTime expected = new DateTime(2013, 2, 28, 0, 0);

        // When
        option.setValueFromArgs(args);
        DateTime result = option.value;

        // Then
        assertThat(result, is(equalTo(expected)));
    }

    @Test
    public void setValueFromArgs_shouldReturnDateTimeFromArgs() throws Exception {
        // Given
        DateOption option = new DateOption();
        String[] args = new String[1];
        args[0] = "-date";

        // When
        option.setValueFromArgs(args);
        DateTime result = option.value;

        // Then
        assertThat(result, is(nullValue()));
    }
}
