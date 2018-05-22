package serializable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.*;


public class TestText {


    @Test
    public void test(){
        Text t = new Text();
        t.set("1234567123456");

        System.out.println(t.find("3",4));
    }


    @Test
    public void testSerialize() throws Exception {

        FileOutputStream fos = new FileOutputStream("/Users/wenhao/tmp/data.dat");
        DataOutputStream dos = new DataOutputStream(fos);

        IntWritable iw = new IntWritable(10);
        iw.write(dos);

        LongWritable lw = new LongWritable(20);
        lw.write(dos);

        Text tx = new Text("hello");
        tx.write(dos);

        tx.set("world");
        tx.write(dos);

        dos.close();
        fos.close();

        FileInputStream fis = new FileInputStream("/Users/wenhao/tmp/data.dat");
        byte[] data = new byte[fis.available()];
        fis.read(data);
        fis.close();

        // int long  text text  反串行化时必须按照顺序才能读取
        ByteArrayInputStream bais = new ByteArrayInputStream(data);

        DataInputStream dis = new DataInputStream(bais);

        iw.readFields(dis);
        System.out.println(iw.get());


        lw.readFields(dis);
        System.out.println(lw.get());

        tx.readFields(dis);
        System.out.println(tx.toString());

        tx.readFields(dis);
        System.out.println(tx.toString());

        bais.close();
        dis.close();
        fis.close();
    }
}
