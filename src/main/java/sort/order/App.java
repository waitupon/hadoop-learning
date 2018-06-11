package sort.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sort.SortAllPartition;


public class App {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage: order <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[1]),true);

        job.setJarByClass(App.class);
        job.setJobName("order App");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setPartitionerClass(SortAllPartition.class);


        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(OrderPartitioner.class);
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        job.setNumReduceTasks(3);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
