package hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/7/25 0025.
 */
public class WordCountReduce extends TableReducer<Text,IntWritable,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                for(IntWritable i :values){
                    count += i.get();
                }

                Put put = new Put(Bytes.toBytes("row-"+key.toString()));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("word"),Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("count"),Bytes.toBytes(count));

                context.write(NullWritable.get(),put);
    }
}
