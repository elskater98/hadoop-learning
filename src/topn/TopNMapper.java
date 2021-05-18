package topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> tweetToRecordMap = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String[] hasthag_times = value.toString().split("\t");
        String hashtag = hasthag_times[0];
        Integer times = Integer.parseInt(hasthag_times[1]);

        tweetToRecordMap.put(times, new Text(hashtag + '\t' + hasthag_times[1]));

        if (tweetToRecordMap.size() > 2 * Integer.parseInt(context.getConfiguration().get("N")))
            tweetToRecordMap.remove(tweetToRecordMap.firstKey());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text text : tweetToRecordMap.values()){
            context.write(NullWritable.get(), text);
        }

    }
}
