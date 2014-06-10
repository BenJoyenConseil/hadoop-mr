package wikipedia.job;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikipedia.domain.PageNameAndCount;
import wikipedia.filters.date.WeekDatePathFilter;
import wikipedia.mappers.SimpleMapper;
import wikipedia.options.DateOption;
import wikipedia.reducers.SimpleReducer;

import java.net.URI;

public class SimpleJob extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SimpleJob(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SimpleJob Wikipedia log");
        job.setJarByClass(SimpleJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPathFilter(job, WeekDatePathFilter.class);
        WeekDatePathFilter.setDateTime(new DateOption(args).getValue());
        TextInputFormat.addInputPath(job, new Path("/in"));
        job.setOutputFormatClass(TextOutputFormat.class);
        Path out = new Path("/out");
        out.getFileSystem(getConf()).delete(out, true);
        TextOutputFormat.setOutputPath(job, out);


        job.setMapperClass(SimpleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageNameAndCount.class);

        job.setCombinerClass(SimpleReducer.class);
        job.setReducerClass(SimpleReducer.class);
        job.setNumReduceTasks(4);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.addCacheFile(new URI("conf/page_names_to_skip.txt"));
        return job.waitForCompletion(true)? 1 : 0;
    }
}
