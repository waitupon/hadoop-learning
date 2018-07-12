package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2018/7/11 0011.
 */
public class TestCURD {
    Connection conn = null;
    Admin admin = null;

    @Before
    public void initConn() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    @Test
    public void addRaw(){
        try {
            Table table = conn.getTable(TableName.valueOf("t1"));
            Put put = new Put(Bytes.toBytes("row1"));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("q1"),Bytes.toBytes("v1"));
            table.put(put);

            table.close();
            System.out.println("add successful");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Test
    public void addList(){
        try {
            Table table = conn.getTable(TableName.valueOf("t1"));
            List<Put> list = new ArrayList<Put>();

            for (int i=3;i<=100;i++){
                Put put = new Put(Bytes.toBytes("row" +i));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("no"),Bytes.toBytes("no" + i));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("tom" + i));
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes( i));
                list.add(put);
            }

            table.put(list);

            table.close();
            System.out.println("addList successful");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void getRaw(){
        try {
            Table table = conn.getTable(TableName.valueOf("t1"));

            Get get = new Get(Bytes.toBytes("row10"));

            Result result = table.get(get);
            String no = Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("no")));
            String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name")));
            int age = Bytes.toInt(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("age")));
            System.out.println("no:" +no +"  name:" +name +"  age:" +age);

            table.close();
            System.out.println("get successful");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Test
    public void findAll(){
        try {
            Table table = conn.getTable(TableName.valueOf("t1"));
            Scan scan = new Scan(Bytes.toBytes("row20"));
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> it = scanner.iterator();

            while(it.hasNext()){
                Result result = it.next();
                String no = Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("no")));
                String name = Bytes.toString(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name")));
                int age = Bytes.toInt(result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("age")));
                System.out.println("no:" +no +"  name:" +name +"  age:" +age);
            }
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void del(){
        try {
            Table table = conn.getTable(TableName.valueOf("t1"));
            Delete delete = new Delete(Bytes.toBytes("row20"));
            delete.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"));
            table.delete(delete);

            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }





}
