package trendingtopic;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class TrendingTopicMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            Pattern pattern = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");

            Matcher matcher = pattern.matcher(value.toString()); // group 0: #gif ; group 1 : gif

            if (matcher.find()) {
                context.write(new Text(matcher.group(1)), one); //{key,value} = {'hashtag':1}
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
