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
            JSONObject obj = new JSONObject(value.toString());
            if (!obj.has("entities") || !obj.has("text"))
                return;

            JSONObject ent = obj.getJSONObject("entities");
            if (!ent.has("hashtags"))
                return;

            JSONArray values = ent.getJSONArray("hashtags");
            if (values.length() == 0)
                return;

            context.write(key, new Text(obj.toString()));

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
