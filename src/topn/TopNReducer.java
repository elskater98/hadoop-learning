package topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> tweetToRecordMap = new TreeMap<>();

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] hasthag_times = value.toString().split("\t");
            String hashtag = hasthag_times[0];
            int times = Integer.parseInt(hasthag_times[1]);

            tweetToRecordMap.put(times, new Text(hashtag + '\t' + hasthag_times[1]));

            if (tweetToRecordMap.size() > Integer.parseInt(context.getConfiguration().get("N")))
                tweetToRecordMap.remove(tweetToRecordMap.firstKey());

        }

        for (Text text : tweetToRecordMap.descendingMap().values()){
            context.write(NullWritable.get(), text);
        }

    }
}
