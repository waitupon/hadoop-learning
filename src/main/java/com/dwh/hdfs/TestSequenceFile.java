package com.dwh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

public class TestSequenceFile {


    @Test
    public void writeSeq() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/usr/my.seq");

        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key = new IntWritable();
        Text value = new Text();

        for(int i=0;i<5;i++){
            key.set(i);
            value.set("itaits" + i);
            writer.append(key,value);
        }
        writer.close();
        System.out.println("over");

    }



    @Test
    public void readSeq() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/usr/my.seq");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();


        while(reader.next(key,value)){
            System.out.println(key +  " = " + value );
        }

        System.out.println("over");

    }
}
