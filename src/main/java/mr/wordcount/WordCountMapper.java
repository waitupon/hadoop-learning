package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.LogUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private Map<String,Integer> words = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        words = new HashMap<String, Integer>();

        context.getCounter("m",LogUtil.getLog("WordCountMapper.setup",this.hashCode())).increment(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(words.size()>=10000){
                sendDataAndCleanup(context);
            }

            String line = value.toString();
            String[] split = line.split(" ");

            for(String str : split){
               addWords(str);
            }
    }

    private void addWords(String str) {
        Integer count = words.get(str);
        if(count == null){
            count = 1;
        }else{
            count += 1;
        }
        words.put(str,count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        sendDataAndCleanup(context);
        context.getCounter("m",LogUtil.getLog("WordCountMapper.cleanup",this.hashCode())).increment(1);
    }

    private void sendDataAndCleanup(Context context) throws IOException, InterruptedException {
        Text text = new Text();
        IntWritable count = new IntWritable();
        for(Map.Entry<String,Integer> map : words.entrySet()){
                text.set(map.getKey());
                count.set(map.getValue());
                context.write(text,count);
        }
    }
}
