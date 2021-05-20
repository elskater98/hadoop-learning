package topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> treeMap = new TreeMap<>();

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String[] line = value.toString().split("\t"); //Format: "hashtag number" 0:hashtag 1:number

            treeMap.put(Integer.parseInt(line[1]), new Text(line[0] + '\t' + line[1]));

            if (treeMap.size() > Integer.parseInt(context.getConfiguration().get("N"))) {
                treeMap.remove(treeMap.firstKey());
            }
        }

        for (Text text : treeMap.descendingMap().values()) {
            context.write(NullWritable.get(), text);
        }

    }
}
