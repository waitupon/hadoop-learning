package hadoop.inputformat.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String line = value.toString();
            String[] split = line.split(" ");

            for(String str : split){
               context.write(new Text(str),new IntWritable(1));
            }
    }






}
