package alljobs;

import cleanup.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import topn.TopN;
import topn.TopNMapper;
import topn.TopNReducer;
import trendingtopic.TrendingTopic;
import trendingtopic.TrendingTopicJSONMapper;
import trendingtopic.TrendingTopicMapper;
import trendingtopic.TrendingTopicReducer;

import java.net.URI;

public class AllJobs {


    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();

        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        JobControl jobctrl = new JobControl("jobcontrol");

        Path inputPath = new Path(args[0]);
        String outputDir = args[1];
        conf.set("N", args[2]);

        // Out Paths
        Path outputPath = new Path(outputDir);
        Path trendingTopicOutputPath = new Path(outputDir + "/trendingtopic");
        Path cleanupOutputPath = new Path(outputDir + "/cleanup");
        Path topnOutputPath = new Path(outputDir + "/topn");


        /* DELETE OUTPUT FOLDERS */
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        fs.delete(outputPath, true);


        /* CLEANUP */
        Job job = Job.getInstance(conf, "Cleanup");
        job.setJarByClass(Cleanup.class);

        ChainMapper.addMapper(job, CorrectFieldsMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, LanguageFilterMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, CustomFieldSelectorMapper.class, LongWritable.class, Text.class, Text.class, Text.class, new Configuration(false));

        ChainMapper.addMapper(job, LowerCaseMapper.class, Text.class, Text.class, Text.class, Text.class, new Configuration(false));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Compress Files
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, cleanupOutputPath);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job);

        /*TRENDING TOPICS*/
        job = Job.getInstance(conf, "Trending Topics");

        job.setJarByClass(TrendingTopic.class);
        job.setMapperClass(TrendingTopicMapper.class);
        job.setReducerClass(TrendingTopicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(job, cleanupOutputPath);
        FileOutputFormat.setOutputPath(job, trendingTopicOutputPath);

        ControlledJob controlledJob2 = new ControlledJob(conf);
        controlledJob2.setJob(job);

        /*TOP N*/
        job = Job.getInstance(conf, "TopN");
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, trendingTopicOutputPath);
        FileOutputFormat.setOutputPath(job, topnOutputPath);

        ControlledJob controlledJob3 = new ControlledJob(conf);
        controlledJob3.setJob(job);


        /*JOBS DEPENDENCIES*/
        jobctrl.addJob(controlledJob1);
        jobctrl.addJob(controlledJob2);
        jobctrl.addJob(controlledJob3);


        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);


        Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();

        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        jobctrl.stop();

        System.out.println("Elapsed: " + (System.currentTimeMillis() - start));
    }
}

class JobRunner implements Runnable {
    private final JobControl control;

    public JobRunner(JobControl _control) {
        this.control = _control;
    }

    public void run() {
        this.control.run();
    }
}
