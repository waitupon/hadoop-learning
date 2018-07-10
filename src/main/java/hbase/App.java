package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/10 0010.
 */
public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("start");
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.zookeeper.quorum", "192.168.3.45");

        Connection conn = ConnectionFactory.createConnection(conf);
        System.out.println("conn");
        Table table = conn.getTable(TableName.valueOf("t1"));

        //设定row no
        Put put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("tom"));

        table.put(put);

        table.close();
        conn.close();
        System.out.println("done");
    }
}
