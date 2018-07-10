package hadoop.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeInputFormat extends FileInputFormat<NullWritable,BytesWritable> {


    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(inputSplit,taskAttemptContext);
        taskAttemptContext.getCounter("in","WholeInputFormat.WholeRecordReader.new").increment(1);
        return recordReader;
    }
}
