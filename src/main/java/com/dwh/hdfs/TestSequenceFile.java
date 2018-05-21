package com.dwh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
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
        Path path = new Path("/usr/my2.seq");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();


        while(reader.next(key,value)){
            System.out.println(key +  " = " + value );
        }

        System.out.println("over");

    }



    @Test
    public void compressSeq() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/usr/my3.seq");

        //SnappyCodec codec = ReflectionUtils.newInstance(SnappyCodec.class,conf);
        DeflateCodec codec = ReflectionUtils.newInstance(DeflateCodec.class,conf);


        SequenceFile.Writer writer = SequenceFile.createWriter(conf,fs.create(path), IntWritable.class, Text.class,SequenceFile.CompressionType.BLOCK,codec);

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



    }
