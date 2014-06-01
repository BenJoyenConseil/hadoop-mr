package wikipedia.options;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AbstractOptionTest {

    @Test
    public void contains_shouldReturnTrue_WhenOptionCodeIsFoundInArgsArray() throws Exception {
        // Given
        String[] args = new String[5];
        args[0] = "efe";
        args[1] = "feeefs";
        args[2] = "-date";
        args[3] = "-searchTopicFile";
        args[4] = ";ezfzef";

        // When
        boolean result = new SearchTopicOption().contains(args);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void contains_shouldReturnFalse_WhenOptionCodeIsNotFoundInArgsArray() throws Exception {
        // Given
        String[] args = new String[5];
        args[0] = "efe";
        args[1] = "feeefs";
        args[2] = "-date";
        args[3] = "-searchTopicFlush";
        args[4] = ";ezfzef";

        // When
        boolean result = new SearchTopicOption().contains(args);

        // Then
        assertThat(result, is(equalTo(false)));
    }
}
