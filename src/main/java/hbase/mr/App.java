package hbase.mr;

import hadoop.mr.maxTemperature.MaxTempApp;
import hadoop.mr.maxTemperature.MaxTemperatureMapper;
import hadoop.mr.maxTemperature.MaxTemperatureReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/25 0025.
 */
public class App {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err
                    .println("Usage: MaxTemperature  <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();

        Configuration conf = job.getConfiguration();
        conf.set("hbase.zookeeper.quorum","node1,node2,node3");
        job.setJarByClass(App.class);
        job.setJobName("Hbase wordCount");


        TableMapReduceUtil.initTableMapperJob("ns1:t1",new Scan(),WordCountMapper.class,Text.class,IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob("ns1:t2",WordCountReduce.class,job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
