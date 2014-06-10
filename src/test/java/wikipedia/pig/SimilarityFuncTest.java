package wikipedia.pig;


import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SimilarityFuncTest {

    @Test
    public void exec_shouldReturnTrue_WhenStr1ContainsStr2_AndIgnoreCase() throws Exception {
        // Given
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append("Wikipedia%3aAdministrateur");
        tuple.append("Wikipedia");

        // When
        boolean result = new SimilarityFunc().exec(tuple);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void exec_shouldReturnFalse_WhenStr1ContainsStr2() throws Exception {
        // Given
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append("blick bluck block");
        tuple.append("black");

        // When
        boolean result = new SimilarityFunc().exec(tuple);

        // Then
        assertThat(result, is(equalTo(false)));
    }

    @Test
    public void exec_shouldReturnTrue_WhenStr1ContainsStr2_withAccent() throws Exception {
        // Given
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append("bl%c3%a9ck");
        tuple.append("bleck");

        // When
        boolean result = new SimilarityFunc().exec(tuple);

        // Then
        assertThat(result, is(equalTo(true)));
    }

    @Test
    public void exec_shouldReturnFalse_WhenTupleContainsNull_AtSecondIndex() throws Exception {
        // Given
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append("bléck");
        tuple.append(null);

        // When
        boolean result = new SimilarityFunc().exec(tuple);

        // Then
        assertThat(result, is(equalTo(false)));
    }
}
