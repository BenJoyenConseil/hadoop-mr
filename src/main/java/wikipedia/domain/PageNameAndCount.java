package wikipedia.domain;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PageNameAndCount implements Writable, Comparable<PageNameAndCount>{
    private String pageName;
    private Long count;


    public PageNameAndCount(){

    }

    public PageNameAndCount(String pageName, Long count){
        this.pageName = pageName;
        this.count = count;
    }

    @Override
    public PageNameAndCount clone() {
        PageNameAndCount result = new PageNameAndCount();
        result.setPageName(pageName);
        result.setCount(count);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pageName);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageName = in.readUTF();
        count = in.readLong();
    }


    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(pageName)
                .append('\t')
                .append(count)
                .toString();
    }

    @Override
    public int compareTo(PageNameAndCount o) {
        return this.getCount().compareTo(o.getCount());
    }
}
