package hadoop.inputformat.db;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text,IntWritable,WordWritable,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable intWritable : values){
            count += intWritable.get();
        }
        WordWritable word = new WordWritable();
        word.setWord(key.toString());
        word.setNum(count);
        context.write(word,NullWritable.get());
    }
}
