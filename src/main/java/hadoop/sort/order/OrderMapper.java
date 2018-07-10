package hadoop.sort.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,ComboKey,Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();

        String path = inputSplit.getPath().toString();

        int flag = 0;
        if(path.contains("customers")){
            flag = 0;
        }else if(path.contains("orders")){
            flag = 1;
        }

        String line = value.toString();
        String[] orderInfo  = line.split("\t");
        String cid = null;
        if(flag == 0){
            cid = orderInfo[0];
        }else if(flag == 1){
            cid = orderInfo[3];
        }
        context.write(new ComboKey(Integer.valueOf(cid),flag),new Text(line));
    }


}
