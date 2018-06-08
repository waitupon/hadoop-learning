package sort.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTempGroupApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]),true);

        job.setJarByClass(MaxTempGroupApp.class);
        job.setJobName("Max temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);


        job.setOutputKeyClass(Combinekey.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区函数
        job.setPartitionerClass(YearPartitioner.class);
        //设置key的排序方式，在达到reduce之前
        job.setSortComparatorClass(SortTemp.class);
        //设置分组对比器
        job.setGroupingComparatorClass(YearGroupComparator.class);

        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
