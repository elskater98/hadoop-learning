package cleanup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class CustomFieldSelectorMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());
            JSONObject json_output = new JSONObject();
            json_output.put("text", json.getString("text"));
            json_output.put("hashtags", json.getJSONObject("entities").getJSONArray("hashtags"));
            json_output.put("lang", json.getString("lang"));

            context.write(new Text(""), new Text(json_output.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
