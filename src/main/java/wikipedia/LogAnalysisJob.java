package wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikipedia.filters.WeekDatePathFilter;
import wikipedia.option.DateOption;
import wikipedia.option.InputPathOption;
import wikipedia.option.OutputPahOption;
import wikipedia.option.SearchTopicOption;

import java.net.URI;

public class LogAnalysisJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new LogAnalysisJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "WikipediaLog");
		job.setJarByClass(LogAnalysisJob.class);
		
		//paramétrage des fichiers d'entrée
		FileNameTextInputFormat.setInputDirRecursive(job, true);
        FileNameTextInputFormat.setInputPaths(job, new InputPathOption(args).getValue());
		
		//paramétrage de la sortie
        Path out = new OutputPahOption(args).getValue();
		out.getFileSystem(getConf()).delete(out, true);
		TextOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(LogMapper.class);
		job.setInputFormatClass(FileNameTextInputFormat.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//Maximisation des performances avec le shuffle
		job.setGroupingComparatorClass(LogGroupComparator.class);
		job.setPartitionerClass(LogPartitioner.class);
		
		job.setReducerClass(LogReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(LongWritable.class);
		
		//Ajout des fichiers de selection langages
		FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile(false, true, new Path("conf/languages_selection.txt"), new Path("languages_selection"));
		job.addCacheFile(new URI("languages_selection"));
		fs.copyFromLocalFile(false, true, new Path("conf/page_names_to_skip.txt"), new Path("page_names_to_skip"));
		job.addCacheFile(new URI("page_names_to_skip"));

        DateOption dateOption = new DateOption();
        if (dateOption.contains(args)) {
            dateOption.setValueFromArgs(args);
            WeekDatePathFilter.setDateTime(dateOption.getValue());
            FileNameTextInputFormat.setInputPathFilter(job, WeekDatePathFilter.class);
        }

        SearchTopicOption searchTopicOption = new SearchTopicOption();
        if (searchTopicOption.contains(args)) {
            searchTopicOption.setValueFromArgs(args);
            fs.copyFromLocalFile(false, true, searchTopicOption.getValue(), new Path("search_topic_dictionary"));
            job.addCacheFile(new URI("search_topic_dictionary"));
            job.setMapperClass(SearchTopicLogMapper.class);
        }

		job.setNumReduceTasks(4);
		
		return job.waitForCompletion(true) ? 1 : 0;
	}

}
