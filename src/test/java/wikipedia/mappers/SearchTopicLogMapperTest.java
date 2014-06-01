package wikipedia.mappers;


import org.junit.Before;
import org.junit.Test;
import wikipedia.domain.CustomKey;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SearchTopicLogMapperTest {

    SearchTopicLogMapper logMapper;

    @Before
    public void setup() {
        logMapper = new SearchTopicLogMapper();
    }

    @Test
    public void isRecordToBeIgnored_shouldReturnFalse_WhenPageNameMatchesTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("PS");
        list.add("UMP");
        logMapper.subjectsToFilter = list;
        CustomKey key = new CustomKey();
        key.setPageName("ps bluck");

        // Then
        boolean result = logMapper.isRecordToBeIgnored(key);

        // When
        assertThat(result, is(equalTo(false)));
    }

    @Test
    public void isRecordToBeIgnored_shouldReturnTrue_WhenPageNameDoesNotMatchesTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("PS");
        list.add("UMP");
        logMapper.subjectsToFilter = list;
        CustomKey key = new CustomKey();
        key.setPageName("Parti des jaunes");

        // Then
        boolean result = logMapper.isRecordToBeIgnored(key);

        // When
        assertThat(result, is(equalTo(true)));
    }


    @Test
    public void isRecordToBeIgnored_shouldReturnFalse_WhenPageNameIsMostLikeTheConf() {
        // Given
        List list = new ArrayList<String>();
        list.add("Union pour un mouvement populaire");
        logMapper.subjectsToFilter = list;
        CustomKey key = new CustomKey();
        key.setPageName("Union_pour_un_mouvement_populaire");

        // Then
        boolean result = logMapper.isRecordToBeIgnored(key);

        // When
        assertThat(result, is(equalTo(false)));
    }
}
