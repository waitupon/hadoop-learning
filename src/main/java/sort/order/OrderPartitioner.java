package sort.order;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<ComboKey,Text> {
    public int getPartition(ComboKey comboKey, Text text, int num) {
        return comboKey.getCid() % num;
    }
}
