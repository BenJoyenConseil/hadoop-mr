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
        String inPathString = "wikipedia";
        if(args.length >= 1)
            inPathString = args[0];
        FileNameTextInputFormat.setInputPaths(job, new Path(inPathString));
		
		//paramétrage de la sortie
        String outPathString = "wikipedia-out";
        if (args.length >= 2)
            outPathString = args[1];
        Path out = new Path(outPathString);
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


        if (args.length >= 3) {
            WeekDatePathFilter.setDateTime(args[2]);
            FileNameTextInputFormat.setInputPathFilter(job, WeekDatePathFilter.class);
        }

        if (args.length >= 4) {
            fs.copyFromLocalFile(false, true, new Path("conf/search_topic_dictionary.txt"), new Path("search_topic_dictionary"));
            job.addCacheFile(new URI("search_topic_dictionary"));
            job.setMapperClass(SearchTopicLogMapper.class);
        }

		job.setNumReduceTasks(5);
		
		return job.waitForCompletion(true) ? 1 : 0;
	}

}
