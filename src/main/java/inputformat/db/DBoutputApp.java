package inputformat.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class DBoutputApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err
                    .println("Usage:  <input path> <output path>");
            System.exit(-1);
        }



        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();

//        FileSystem fs = FileSystem.get(conf);
//        fs.delete(new Path(args[1]),true);

        job.setJarByClass(DBoutputApp.class);
        job.setJobName("db import");
        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setOutputFormatClass(DBOutputFormat.class);

        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.6.14:3306/dwh","xiang","nEw-TESt@&2#");

        DBOutputFormat.setOutput(job,"words","word","num");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(WordWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
