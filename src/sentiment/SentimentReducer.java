package sentiment;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentimentReducer extends Reducer<Text, SentimentWritable, Text, Text> {
    public void reduce(Text key, Iterable<SentimentWritable> values, Context context) throws IOException, InterruptedException {
        float ratio = 0;
        for (SentimentWritable value : values) {
            ratio += (float) value.getRatio().get() / (float) value.getLength().get();
        }

        if (ratio != 0) {
            ratio = Math.abs(ratio) * 100;
            context.write(key, new Text(ratio + "% " + (ratio < 0 ? "negative" : "positive")));
        }
    }
}
