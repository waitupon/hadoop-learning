package hadoop.inputformat.db;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DbImprotApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage:  <input path> <output path>");
            System.exit(-1);
        }



        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        job.setJarByClass(DbImprotApp.class);
        job.setJobName("db import");
        /*FileInputFormat.addInputPath(job, new Path(args[0]));*/
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.6.14:3306/dwh","xiang","nEw-TESt@&2#");
        DBInputFormat.setInput(job,UserWritable.class,"select id,name from base_user","select count(*) from base_user");

        job.setInputFormatClass(DBInputFormat.class);

        job.setMapperClass(DbImportMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
