package sentiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class Sentiment extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String positivePath = args[2];
        String negativePath = args[3];

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Sentiment");
        job.addCacheFile(new URI(positivePath));
        job.addCacheFile(new URI(negativePath));

        job.setJarByClass(getClass());
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SentimentWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Sentiment(), args);
        System.exit(exitCode);
    }
}
