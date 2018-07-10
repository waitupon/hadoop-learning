package hadoop.mr.maxTemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.InetAddress;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private static final int MISSING = 9999;

    InetAddress addr = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        addr = InetAddress.getLocalHost();
        context.getCounter("m","maxMapper.setup." + addr).increment(1);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,21);
        int airTemp = 0;
        if(line.charAt(87) == '+'){
            airTemp = Integer.parseInt(line.substring(88,92));
        }else{
            airTemp = Integer.parseInt(line.substring(87,92));
        }
        String quality = line.substring(92,93);
        if(airTemp!=MISSING && quality.matches("[01459]")){
            context.write(new Text(year),new IntWritable(airTemp));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("m","maxMapper.cleanup." + addr).increment(1);
    }
}
