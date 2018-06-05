package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTempApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }
//        Configuration conf = new Configuration();
//        conf.set("mapred.jar", "MaxTemperature.jar");

        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]),true);

        job.setJarByClass(MaxTempApp.class);
        job.setJobName("Max temperature");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setPartitionerClass(SortAllPartition.class);

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);



//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(4);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
