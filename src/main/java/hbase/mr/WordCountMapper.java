package hbase.mr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;


/**
 * Created by Administrator on 2018/7/25 0025.
 */
public class WordCountMapper extends TableMapper<Text,IntWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String word = Bytes.toString(value.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name")));
        context.write(new Text(word),new IntWritable(1));
    }
}
