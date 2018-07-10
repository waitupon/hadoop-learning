package hadoop.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by Administrator on 2018/5/30 0030.
 */
public class MyPartition extends Partitioner<Text,IntWritable> {
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return 0;
    }


}
