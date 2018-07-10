package hadoop.sort.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<ComboKey,Text,Text,NullWritable> {


    @Override
    protected void reduce(ComboKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator =values.iterator();
        String customer = iterator.next().toString();
        int cid = key.getCid();

        while(iterator.hasNext()){
            Text order = iterator.next();
            String info = customer + "    " +  order.toString();
            context.write(new Text(info),NullWritable.get());
        }
    }


}
