package wikipedia.utils;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class UTF8DecoderTest {
    @Test
    public void unescape_ShouldReplaceSpecialCharacters() throws Exception {
        // Given
        String pageNameUnFormated = "jean-fran%c3%a7ois cop%c3%a9";
        String expected = "jean-françois copé";

        // When
        String result = UTF8Decoder.unescape(pageNameUnFormated);

        // Then
        assertThat(result, is(equalTo(expected)));
    }
}
