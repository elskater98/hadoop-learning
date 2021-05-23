package trendingtopic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class TrendingTopicJSONMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());
            JSONArray values = json.getJSONArray("hashtags");
            for (int i = 0; i < values.length(); i++) {

                JSONObject hashtag = values.getJSONObject(i);
                String text = hashtag.getString("text");

                if (text.isEmpty()) {
                    continue;
                }

                context.write(new Text(text), one);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
