package wikipedia.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import wikipedia.FileNameTextInputFormat;
import wikipedia.domain.CustomKey;
import wikipedia.filters.date.DatePathFilter;
import wikipedia.filters.date.DayDatePathFilter;
import wikipedia.mappers.LogMapper;

import java.io.IOException;

public class FileNameTextLoadFunc extends LoadFunc{
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private FileNameTextInputFormat.FileNameTextRecordReader reader = null;
    private String dateStr = "";
    private Text currentKey;
    private Text currentValue;
    private LogMapper logMapper;

    public FileNameTextLoadFunc(){
        currentKey = new Text();
        currentValue = new Text();
        logMapper = new LogMapper();
    }

    public FileNameTextLoadFunc(String dateTimeString){
        this();
        dateStr = dateTimeString;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileNameTextInputFormat.setInputPaths(job, location);

        if(!dateStr.isEmpty()){
            DatePathFilter.setDateTime(dateStr);
            FileNameTextInputFormat.setInputPathFilter(job, DayDatePathFilter.class);
        }
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new FileNameTextInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.reader = (FileNameTextInputFormat.FileNameTextRecordReader)recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
    	Tuple defaultTuple = tupleFactory.newTuple(4);
        try {
            if(!reader.nextKeyValue())
                return null;
            currentKey.set(reader.getCurrentKey());
            currentValue.set(reader.getCurrentValue());
            String[] fileNameSplits = StringUtils.split(currentKey.toString(), '-');
            String[] recordSplits = StringUtils.split(currentValue.toString(), ' ');
            if(fileNameSplits.length < 2 || recordSplits.length < 4){
            	return defaultTuple;
            }

            CustomKey item = logMapper.buildCustomKeyFromRecord(fileNameSplits[1], recordSplits);
            Tuple tuple = tupleFactory.newTuple();
            tuple.append(currentKey.toString());
            tuple.append(item.getLang());
            tuple.append(item.getPageName());
            tuple.append(item.getCount());

            return tuple;

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
		return defaultTuple;
    }
}
