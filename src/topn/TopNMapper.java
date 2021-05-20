package topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final TreeMap<Integer, Text> treeMap = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        String[] line = value.toString().split("\t"); //Format: "hashtag number" 0:hashtag 1:number

        treeMap.put(Integer.parseInt(line[1]), new Text(line[0] + '\t' + line[1]));
        if (treeMap.size() > 2 * Integer.parseInt(context.getConfiguration().get("N"))) {
            treeMap.remove(treeMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text text : treeMap.values()) {
            context.write(NullWritable.get(), text);
        }

    }
}
