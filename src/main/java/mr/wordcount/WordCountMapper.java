package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.InetAddress;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final int MISSING = 9999;

    InetAddress addr = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        addr = InetAddress.getLocalHost();
        context.getCounter("m","WordCountMapper.setup." + addr).increment(1);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");

            for(String str : split){
                context.write(new Text(str),new IntWritable(1));
            }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("m","maxMapper.cleanup." + addr).increment(1);
    }
}
