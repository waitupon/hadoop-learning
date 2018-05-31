package inputformat.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2018/5/31 0031.
 */
public class DbImportMapper extends Mapper<LongWritable,UserWritable,Text,Text> {

    @Override
    protected void map(LongWritable key, UserWritable value, Context context) throws IOException, InterruptedException {
        Integer id = value.getId();
        String name = value.getName();

        context.write(new Text(id.toString()),new Text(name));

    }

}
