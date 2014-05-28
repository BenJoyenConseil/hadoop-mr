package wikipedia.utils;

import java.text.Normalizer;

public class ASCIINormalizer {
    public static String formatStringNormalizer(String s) {
        String temp = Normalizer.normalize(s, Normalizer.Form.NFD);
        return temp.replaceAll("[^\\p{ASCII}]", "").toLowerCase();
    }
}
