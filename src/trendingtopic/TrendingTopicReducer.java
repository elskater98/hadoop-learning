package trendingtopic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrendingTopicReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int total = 0;
        for (IntWritable val : values)
            total += Integer.parseInt(val.toString());
        // Write reduce result {word,count}
        context.write(new Text(key.toString()), new IntWritable(total));
    }
}
