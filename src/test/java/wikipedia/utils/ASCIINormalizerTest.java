package wikipedia.utils;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ASCIINormalizerTest {

    @Test
    public void formatStringNormalizer() {

        // Given
        String in = "jean françois copé";
        String expected = "jean francois cope";

        // When
        String result = ASCIINormalizer.formatStringNormalizer(in);

        // Then
        assertThat(result, is(equalTo(expected)));
    }
}
