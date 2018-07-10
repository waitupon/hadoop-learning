package hadoop.sort.secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<Combinekey,NullWritable> {

    public int getPartition(Combinekey combinekey, NullWritable nullWritable, int num) {
        return (combinekey.getYear() * 127) % num;
    }
}
