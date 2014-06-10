package wikipedia.domain;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {

	private int day;
	private int month;
	private int year;
	private String lang;
	private String pageName;
	private Long count;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(day);
		out.writeInt(month);
		out.writeInt(year);
		out.writeUTF(lang);
		out.writeUTF(pageName);
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		day = in.readInt();
		month = in.readInt();
		year = in.readInt();
		lang = in.readUTF();
		pageName = in.readUTF();
		count = in.readLong();
	}

	@Override
	public int compareTo(CustomKey o) {
		int result = this.year - o.year;
		if(result == 0){
			result = this.month - o.month;
			if(result == 0){
				result = this.lang.compareTo(o.lang);
				if(result == 0){
					result = this.pageName.compareTo(o.pageName);
				}
			}
		}
		return result;
	}
	
	@Override
	public boolean equals(Object o){
		
		if(!(o instanceof CustomKey))
				return false;
		
		return o.hashCode() == hashCode();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder()
		.append(lang)
		.append(day)
		.append(month)
		.append(year)
		.append(count)
		.append(pageName).hashCode();
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
						.append(year)
						.append("-")
						.append(month)
						.append(" : ")
						.append(lang)
						.append("\t")
						.append(pageName)
						.toString();
	}
	
	@Override
	public Object clone() {
		CustomKey key = new CustomKey();
		key.setCount(count);
		key.setLang(lang);
		key.setDay(day);
		key.setMonth(month);
		key.setYear(year);
		key.setPageName(pageName);
		return key;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = lang;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
