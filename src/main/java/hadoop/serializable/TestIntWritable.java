package hadoop.serializable;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

public class TestIntWritable {

    @Test
    public void serialize() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);

        IntWritable i = new IntWritable(22);
        i.write(oos);  //oos.write(this.value)

        oos.close();
        baos.close();

        System.out.println(baos.toByteArray().length);
    }
}
