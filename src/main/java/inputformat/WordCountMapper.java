package inputformat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import utils.LogUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordCountMapper extends Mapper<NullWritable,BytesWritable,Text,IntWritable> {

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        IntWritable one = new IntWritable(1);
        byte[] bytes = value.copyBytes();
        String str = new String(bytes,"utf8");
        String[] split = str.replaceAll("\n", " ").split(" ");

        for(String s : split){
            text.set(s);
            context.write(text,one);
        }
    }
}
