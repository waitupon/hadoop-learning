package com.dwh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2018/5/16 0016.
 */
public class TestFileSystem {

    @Test
    public void testWrite() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream dos = fs.create(new Path("hdfs://master:9000/usr/hello.txt"));

        dos.write("hello".getBytes());
        dos.close();
        System.out.println("over");

    }


}
