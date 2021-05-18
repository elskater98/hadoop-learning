package trendingtopic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class TrendingTopic extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Trending Topic");

        // Set Driver
        job.setJarByClass(getClass());

        // Set Mapper
        job.setMapperClass(TrendingTopicMapper.class);

        // Set Reducer
        job.setReducerClass(TrendingTopicReducer.class);

        // Set Output Key and Value; {"#hadoop":1}
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Get arguments and setting paths
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TrendingTopic(), args);
        System.exit(exitCode);
    }
}

