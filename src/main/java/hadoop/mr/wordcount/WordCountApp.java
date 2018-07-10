package hadoop.mr.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCountApp {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err
                    .println("Usage: wordCount <input path> <output path>");
            System.exit(-1);
        }
//        Configuration conf = new Configuration();
//        conf.set("mapred.jar", "MaxTemperature.jar");
        Job job = Job.getInstance();
        job.setJarByClass(WordCountApp.class);
        job.setJobName("wordCount");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //FileInputFormat.setMaxInputSplitSize(job,100);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setCombinerClass(WordCountReducer.class); // 设置联合reducer

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextOutputFormat.setCompressOutput(job,true);
        TextOutputFormat.setOutputCompressorClass(job,DeflateCodec.class);

        //job.setPartitionerClass(MyPartition.class); //设置分区
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
