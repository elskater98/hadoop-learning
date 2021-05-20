package cleanup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class Cleanup extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Cleanup");
        job.setJarByClass(getClass());

        // Chain Mapper ( CorrectFields -> LanguageFilter -> Select-> LowerCase)
        ChainMapper.addMapper(job, CorrectFieldsMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, LanguageFilterMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, CustomFieldSelectorMapper.class, LongWritable.class, Text.class, Text.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, LowerCaseMapper.class, Text.class, Text.class, Text.class, Text.class, new Configuration(false));

        // Setting the input and output path
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Cleanup(), args);
        System.exit(exitCode);
    }
}

