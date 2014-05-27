package wikipedia;

import java.util.Comparator;

public class CountComparator implements Comparator<CustomKey>{

	@Override
	public int compare(CustomKey arg0, CustomKey arg1) {
		return (int)(arg0.getCount() - arg1.getCount());
	}

}
