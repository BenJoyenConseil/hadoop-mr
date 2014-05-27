package wikipedia;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
		FileNameTextInputFormat.setInputPaths(job, new Path("wikipedia"));
		
		//paramétrage de la sortie
		Path out = new Path("wikipedia-out");
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
		fs.copyFromLocalFile(false, true, new Path("languages_selection.txt"), new Path("languages_selection.txt"));
		job.addCacheFile(new URI("languages_selection.txt"));
		fs.copyFromLocalFile(false, true, new Path("page_names_to_skip.txt"), new Path("page_names_to_skip.txt"));
		job.addCacheFile(new URI("page_names_to_skip.txt"));
		
		job.setNumReduceTasks(5);
		
		return job.waitForCompletion(true) ? 1 : 0;
	}

}
