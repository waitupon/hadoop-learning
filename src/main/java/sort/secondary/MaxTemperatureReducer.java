package sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.InetAddress;

public class MaxTemperatureReducer extends Reducer<Combinekey,NullWritable,Combinekey,NullWritable> {

    @Override
    protected void reduce(Combinekey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }


}
