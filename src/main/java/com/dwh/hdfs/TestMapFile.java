package com.dwh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;


public class TestMapFile {


    @Test
    public void write() throws Exception {
        Configuration conf = new Configuration();
        //设置为本地
        conf.set("fs.defaultFS","file:///");

        FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = new MapFile.Writer(conf,fs,"/Users/wenhao/tmp/mymap.map",IntWritable.class,Text.class);

        writer.append(new IntWritable(1),new Text("itaits1"));
        writer.append(new IntWritable(2),new Text("itaits2"));
        writer.append(new IntWritable(4),new Text("itaits4"));

        writer.close();
    }


    @Test
    public void read() throws Exception {
        Configuration conf = new Configuration();
        //设置为本地
        conf.set("fs.defaultFS","file:///");

        FileSystem fs = FileSystem.get(conf);
        MapFile.Reader reader = new MapFile.Reader(fs,"/Users/wenhao/tmp/mymap.map",conf);
        Text value = new Text();

        reader.get( new IntWritable(3),value);
        System.out.println(value);
        reader.close();
    }
}
