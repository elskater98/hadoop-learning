package cleanup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class CustomFieldSelectorMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject obj = new JSONObject(value.toString());
            JSONObject clean = new JSONObject();
            clean.put("text", obj.getString("text"));
            clean.put("hashtags", obj.getJSONObject("entities").getJSONArray("hashtags"));
            clean.put("lang", obj.getString("lang"));

            context.write(key, new Text(clean.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
