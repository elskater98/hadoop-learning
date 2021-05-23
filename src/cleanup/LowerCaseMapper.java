package cleanup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;

public class LowerCaseMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        try {
            JSONObject json = new JSONObject(value.toString());
            if (json.has("text")) {
                json.put("text", json.getString("text").toLowerCase());
            }

            context.write(new Text(""), new Text(json.toString()));
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
