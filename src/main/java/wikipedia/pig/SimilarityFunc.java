package wikipedia.pig;


import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import wikipedia.utils.ASCIINormalizer;
import wikipedia.utils.UTF8Decoder;

import java.io.IOException;
import java.util.Iterator;

public class SimilarityFunc extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if(input.size() < 2)
            return false;

        String str1, str2;
        Iterator<Object> iterator = input.iterator();
        Object o1 = iterator.next();
        Object o2 = iterator.next();
        if(o1 == null || o2 == null) {
            return false;
        }

        str1 = ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(o1.toString()));
        str2 = ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(o2.toString()));

        return str1.contains(str2);
    }
}
