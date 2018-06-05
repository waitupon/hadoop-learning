package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.InetAddress;

public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    InetAddress addr = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        addr = InetAddress.getLocalHost();
        context.getCounter("r","maxMapper.setup." + addr).increment(1);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for(IntWritable value : values){
            maxValue = Math.max(maxValue,value.get());
        }
        context.write(key,new IntWritable(maxValue));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("r","maxMapper.cleanup." + addr).increment(1);
    }
}
