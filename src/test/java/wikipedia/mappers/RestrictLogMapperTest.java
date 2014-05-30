package wikipedia.mappers;


import org.junit.Test;
import wikipedia.CustomKey;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RestrictLogMapperTest {

    RestrictLogMapper logMapper = new RestrictLogMapper();

    @Test
    public void isRecordToBeIgnored_shouldReturnTrue_WhenPageNameDoesNotMatchTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("Special");
        list.add("Undefined");
        logMapper.subjectsToIgnore = list;
        CustomKey key = new CustomKey();
        key.setPageName("Undefined");

        // Then
        boolean result = logMapper.isRecordToBeIgnored(key);

        // When
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void isRecordToBeIgnored_shouldReturnFalse_WhenPageNameMatchesTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("Special");
        list.add("undefined");
        logMapper.subjectsToIgnore = list;
        CustomKey key = new CustomKey();
        key.setPageName("bluckblcuk");

        // Then
        boolean result = logMapper.isRecordToBeIgnored(key);

        // When
        assertThat(result, is(equalTo(false)));
    }
}
