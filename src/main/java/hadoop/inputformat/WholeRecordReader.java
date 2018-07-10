package hadoop.inputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * 一次把文件的内容全部加载
 */
public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private FileSplit fileSplit;
    private boolean processed = false;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
         fileSplit = (FileSplit) inputSplit;
         configuration =  taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            byte[]contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(configuration);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in,contents,0,contents.length);
                value.set(contents,0,contents.length);
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
