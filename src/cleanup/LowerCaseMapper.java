package cleanup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject obj = new JSONObject(value.toString());
            if (obj.has("text"))
                obj.put("text", obj.getString("text").toLowerCase());

            context.write(key, new Text(obj.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
