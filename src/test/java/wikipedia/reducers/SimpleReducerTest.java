package wikipedia.reducers;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import wikipedia.domain.PageNameAndCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class SimpleReducerTest {
    ReduceDriver<Text, PageNameAndCount, Text, PageNameAndCount> reduceDriver;

    @Before
    public void setup() {
        reduceDriver = ReduceDriver.newReduceDriver(new SimpleReducer());
    }

    @Test
    public void addKeyValueToTopMap_shouldOrderByDesc() throws IOException, InterruptedException {
        // Given
        SimpleReducer reducer = new SimpleReducer();
        Reducer.Context context = mock(Reducer.Context.class);
        reducer.setup(context);

        String lang = "fr";
        PageNameAndCount item1 = new PageNameAndCount();
        item1.setPageName("bluck");
        item1.setCount(134L);
        PageNameAndCount item2 = new PageNameAndCount();
        item2.setPageName("black");
        item2.setCount(13L);

        PageNameAndCount item3 = new PageNameAndCount();
        item3.setPageName("block");
        item3.setCount(145L);

        // When
        reducer.addKeyValueToTopMap(lang, item1);
        reducer.addKeyValueToTopMap(lang, item3);
        reducer.addKeyValueToTopMap(lang, item2);
        PageNameAndCount min = reducer.topTenByLang.get("fr").first();

        // Then
        assertThat(min.getCount(), is(equalTo(13L)));
    }

    @Test
    public void addKeyValueToTopMap_shouldSummerizeCountByPageName() throws IOException, InterruptedException {
        // Given
        SimpleReducer reducer = new SimpleReducer();
        Reducer.Context context = mock(Reducer.Context.class);
        reducer.setup(context);

        String lang = "fr";
        String pageName = "bluck";

        PageNameAndCount item1 = new PageNameAndCount();
        PageNameAndCount item2 = new PageNameAndCount();
        item1.setPageName(pageName);
        item2.setPageName(pageName);
        item1.setCount(134L);
        item2.setCount(13L);
        Long expected = 147L;

        // When
        reducer.addKeyValueToTopMap(lang, item1);
        reducer.addKeyValueToTopMap(lang, item2);
        PageNameAndCount result = reducer.topTenByLang.get("fr").first();

        // Then
        assertThat(reducer.topTenByLang.get(lang).size(), is(equalTo(1)));
        assertThat(result.getPageName(), is(equalTo(pageName)));
        assertThat(result.getCount(), is(equalTo(expected)));
    }

    @Ignore
    @Test
    public void reduce_ShouldRetrieveTopTenViewed_WikipediaPage() throws IOException {
        // Given
        Text keyIn = new Text("fr");
        List<PageNameAndCount> valuesIn = new ArrayList<PageNameAndCount>();
        PageNameAndCount item1 = new PageNameAndCount();
        item1.setPageName("bluck");
        item1.setCount(2L);
        valuesIn.add(item1);

        PageNameAndCount item2 = new PageNameAndCount();
        item2.setPageName("flush");
        item2.setCount(4L);
        valuesIn.add(item2);

        PageNameAndCount item3 = new PageNameAndCount();
        item3.setPageName("bluck");
        item3.setCount(8L);
        valuesIn.add(item3);

        reduceDriver.withInput(keyIn, valuesIn);

        ArrayList<Pair<Text, PageNameAndCount>> outputRecords = new ArrayList<Pair<Text, PageNameAndCount>>();
        Pair pair1 = new Pair(new Text("fr"), new PageNameAndCount("bluck", 10L));
        Pair pair2 = new Pair(new Text("fr"), new PageNameAndCount("flush", 4L));
        outputRecords.add(pair1);
        outputRecords.add(pair2);
        reduceDriver.withAllOutput(outputRecords);

        // When
        reduceDriver.runTest();

        // Then
    }
}
