package sentiment;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class SentimentMapper extends Mapper<Object, Text, Text, SentimentWritable> {
    private final Set<String> positive = new HashSet<>();
    private final Set<String> negative = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] wordFiles = context.getCacheFiles();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(wordFiles[0].getPath()));
            String word;
            while ((word = bufferedReader.readLine()) != null)
                positive.add(word.toLowerCase());

            bufferedReader = new BufferedReader(new FileReader(wordFiles[1].getPath()));
            while ((word = bufferedReader.readLine()) != null)
                negative.add(word.toLowerCase());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        try {
            Pattern pattern = Pattern.compile("(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");
            JSONObject json = new JSONObject(value.toString());
            String[] words = json.getString("text").split(" ");
            JSONArray hashtags = json.getJSONArray("hashtags");
            int tweetLen = words.length;
            int positiveCount = 0;
            int negativeCount = 0;

            for (String word : words) {
                if (pattern.matcher(word).find()) {
                    continue;
                }

                if (positive.contains(word)) {
                    positiveCount++;
                }


                if (negative.contains(word)) {
                    negativeCount++;
                }

            }

            for (int i = 0; i < hashtags.length(); i++) {
                JSONObject hasthag = hashtags.getJSONObject(i);
                String text = hasthag.getString("text");
                context.write(new Text(text),
                        new SentimentWritable(new IntWritable(positiveCount - negativeCount), new IntWritable(tweetLen)));
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
