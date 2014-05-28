package wikipedia;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class FileNameTextInputFormat extends FileInputFormat<Text, Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		return new FileNameTextRecordReader();

	}
	
	public static class FileNameTextRecordReader extends RecordReader<Text, Text>{

		private Text currentKey;
		private Text currentValue;
		private Path filePath;
		private LineRecordReader recordReader;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit inputsplit = (FileSplit)split;
			filePath = inputsplit.getPath();
			currentKey = new Text(filePath.getName());
			currentValue = new Text();
			
		    String delimiter = context.getConfiguration().get(
		        "textinputformat.record.delimiter");
		    byte[] recordDelimiterBytes = null;
		    if (null != delimiter)
		      recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
		    recordReader = new LineRecordReader(recordDelimiterBytes);
		    recordReader.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(recordReader.nextKeyValue()){
				currentValue = recordReader.getCurrentValue();
				return true;
			}
			return false;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return recordReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			recordReader.close();
		}
		
	}
}
