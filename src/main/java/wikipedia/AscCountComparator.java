package wikipedia;

import wikipedia.domain.CustomKey;

import java.util.Comparator;

public class AscCountComparator implements Comparator<CustomKey>{

	@Override
	public int compare(CustomKey arg0, CustomKey arg1) {
		return arg0.getCount().compareTo(arg1.getCount());
	}

}
