package sentiment;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SentimentWritable implements Writable {

    private final IntWritable ratio;
    private final IntWritable length;

    // Default constructor
    public SentimentWritable() {
        this.ratio = new IntWritable(0);
        this.length = new IntWritable(0);
    }

    public SentimentWritable(IntWritable ratio, IntWritable len) {
        this.ratio = ratio;
        this.length = len;
    }

    public IntWritable getRatio() {
        return ratio;
    }

    public IntWritable getLength() {
        return length;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        length.write(dataOutput);
        ratio.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length.readFields(dataInput);
        ratio.readFields(dataInput);
    }
}
