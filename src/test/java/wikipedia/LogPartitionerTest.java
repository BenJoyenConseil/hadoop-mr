package wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class LogPartitionerTest {

    @Test
    public void getPartition() throws Exception {
        // Given
        CustomKey fr = build("fr");
        CustomKey en = build("en");
        CustomKey de = build("de");
        CustomKey es = build("es");
        LongWritable value = null;
        int numPartition = 4;

        // When
        int result1 = new LogPartitioner().getPartition(fr, value, numPartition);
        int result2 = new LogPartitioner().getPartition(en, value, numPartition);
        int result3 = new LogPartitioner().getPartition(de, value, numPartition);
        int result4 = new LogPartitioner().getPartition(es, value, numPartition);

        // Then
        assertThat(result1, is(equalTo(0)));
        assertThat(result2, is(equalTo(1)));
        assertThat(result3, is(equalTo(1)));
        assertThat(result4, is(equalTo(2)));

    }

    CustomKey build(String lang){
        CustomKey customKey = new CustomKey();
        customKey.setLang(lang);
        return customKey;
    }
}
