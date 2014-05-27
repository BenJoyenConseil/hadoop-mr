package wikipedia;

import org.apache.hadoop.io.WritableComparator;

public class LogGroupComparator extends WritableComparator {
	
	public LogGroupComparator(){
		super(CustomKey.class, true);
	}

	@Override
	public int compare(Object a, Object b) {
		CustomKey k1 = (CustomKey)a;
		CustomKey k2 = (CustomKey)b;
		return k1.compareTo(k2);
	}
}
