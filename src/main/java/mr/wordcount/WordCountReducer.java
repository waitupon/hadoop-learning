package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    InetAddress addr = null;

    Map<String,Integer> data =  null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        addr = InetAddress.getLocalHost();
        context.getCounter("r","WordCountReducer.setup." + addr).increment(1);
        data = new HashMap<String, Integer>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;


        Integer v = 0;
        int v2 = 0;
        if(data.containsKey(key.toString())){
             v = data.get(key.toString());
        }
        for(IntWritable writable : values){
            v2 += writable.get();
        }
        v = v2+v;
        context.write(key,new IntWritable(v));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("r","WordCountReducer.cleanup." + addr).increment(1);
    }
}
