package cleanup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class CorrectFieldsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());
            if (!json.has("entities") || !json.has("text"))
                return;

            JSONObject entities = json.getJSONObject("entities");
            if (!entities.has("hashtags"))
                return;

            JSONArray hashtags = entities.getJSONArray("hashtags");
            if (hashtags.length() == 0)
                return;

            context.write(key, new Text(json.toString()));

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
