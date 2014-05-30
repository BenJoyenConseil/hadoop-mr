package wikipedia.option;


import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class OptionCollectionTest {

    @Test
    public void toString_should(){
        // Given
        String[] args = new String[1];
        args[0] = "-help";

        // When
        String result = new OptionCollection(args).toString();

        // Then
        assertThat(result, is(equalTo("-in\n" +
                                    "-out\n" +
                                    "-searchTopicFile\n" +
                                    "-restrictionFile\n" +
                                    "-date\n" +
                                    "-noTopTen\n" +
                                    "-help\n")));
    }
}
