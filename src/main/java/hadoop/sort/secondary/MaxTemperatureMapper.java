package hadoop.sort.secondary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable,Text,Combinekey,NullWritable> {
    private static final int MISSING = 9999;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,19);
        int airTemp = 0;
        if(line.charAt(87) == '+'){
            airTemp = Integer.parseInt(line.substring(88,92));
        }else{
            airTemp = Integer.parseInt(line.substring(87,92));
        }
        String quality = line.substring(92,93);
        if(airTemp!=MISSING && quality.matches("[01459]")){
            context.write(new Combinekey(new Integer(year),new Integer(airTemp)), NullWritable.get());
        }
    }

}
