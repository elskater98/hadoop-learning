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
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        try {
            JSONObject obj = new JSONObject(value.toString());
            JSONArray values = obj.getJSONArray("hashtags");
            for (int i = 0; i < values.length(); i++) {

                JSONObject hasthag = values.getJSONObject(i);
                String text = hasthag.getString("text");
                if (text.isEmpty())
                    continue;

                context.write(new Text(text), one);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
