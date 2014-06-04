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
        String str1, str2;
        if(input.size() < 2)
            return false;

        Iterator<Object> iterator = input.iterator();
        str1 = ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(iterator.next().toString()));
        str2 = ASCIINormalizer.formatStringNormalizer(UTF8Decoder.unescape(iterator.next().toString()));


        return str1.contains(str2);
    }
}
