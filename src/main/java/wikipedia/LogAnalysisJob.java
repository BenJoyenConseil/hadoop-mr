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
import wikipedia.domain.CustomKey;
import wikipedia.filters.date.DayDatePathFilter;
import wikipedia.mappers.LogMapper;
import wikipedia.mappers.RestrictLogMapper;
import wikipedia.mappers.SearchTopicLogMapper;
import wikipedia.options.*;
import wikipedia.reducers.LogReducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LogAnalysisJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new LogAnalysisJob(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "WikipediaLog");
		job.setJarByClass(LogAnalysisJob.class);

        OptionCollection options = new OptionCollection(args);
        if(options.contains(OptionType.Help)){
            System.out.println(options.toString());
            return 0;
        }

        setInAndOut(job, options);
        setMapReduceClasses(job);
        setLangSelection(job);

        if (options.contains(OptionType.RestrictSearch)) {
            setRestrictionFilter(options, job);
        }else if (options.contains(OptionType.SearchTopic)) {
            setSearchTopicsFilter(options, job);
        }
        if (options.contains(OptionType.Date)) {
            setDateFilter(options, job);
        }
        if(options.contains(OptionType.NoTopTen)){
            LogMapper.setTopTenMapper(false);
        }
        if(options.contains(OptionType.UseCombiner)){
            job.setCombinerClass(LogReducer.class);
        }

		job.setNumReduceTasks(4);
		return job.waitForCompletion(true) ? 1 : 0;
	}



    private void setDateFilter(OptionCollection options, Job job) {
        DateOption dateOption = (DateOption)options.get(OptionType.Date);
        DayDatePathFilter.setDateTime(dateOption.getValue());
        FileNameTextInputFormat.setInputPathFilter(job, DayDatePathFilter.class);
    }

    private void setSearchTopicsFilter(OptionCollection options, Job job) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(getConf());
        SearchTopicOption option = (SearchTopicOption)options.get(OptionType.SearchTopic);
        fs.copyFromLocalFile(false, true, option.getValue(), new Path("search_topic_dictionary"));
        job.addCacheFile(new URI("search_topic_dictionary"));
        job.setMapperClass(SearchTopicLogMapper.class);
    }

    private void setRestrictionFilter(OptionCollection options, Job job) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(getConf());
        RestrictSearchOption option = (RestrictSearchOption) options.get(OptionType.RestrictSearch);
        fs.copyFromLocalFile(false, true, option.getValue(), new Path("page_names_to_skip"));
        job.addCacheFile(new URI("page_names_to_skip"));
        job.setMapperClass(RestrictLogMapper.class);
    }

    private void setLangSelection(Job job) throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(getConf());
        fs.copyFromLocalFile(false, true, new Path("conf/languages_selection.txt"), new Path("languages_selection"));
        job.addCacheFile(new URI("languages_selection"));
    }

    private void setMapReduceClasses(Job job) {
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
    }

    private void setInAndOut(Job job, OptionCollection options) throws IOException {
        //paramétrage des fichiers d'entrée
        FileNameTextInputFormat.setInputDirRecursive(job, true);
        Path in = (Path) options.get(OptionType.InputPath).getValue();
        FileNameTextInputFormat.setInputPaths(job, in);

        //paramétrage de la sortie
        Path out = (Path)options.get(OptionType.OutputPath).getValue();
        out.getFileSystem(getConf()).delete(out, true);
        TextOutputFormat.setOutputPath(job, out);
    }

}
