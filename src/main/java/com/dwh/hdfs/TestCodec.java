package com.dwh.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class TestCodec {


    @Test
    public void testcompress() throws Exception {
        String codecClassName = "org.apache.hadoop.io.compress.DefaultCodec";

        Class<?>codecClass = Class.forName(codecClassName);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        FileInputStream fis = new FileInputStream("/Users/wenhao/tmp/error-2018-04-20.log");

        FileOutputStream fos = new FileOutputStream("/Users/wenhao/tmp/log.deflate");
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copy(fis,cos,1024);
        cos.flush();

        cos.close();
        fos.close();
        fis.close();
        System.out.println("over");

    }


    @Test
    public void testDeCompress() throws Exception {
        Configuration conf = new Configuration();
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(new Path("/Users/wenhao/tmp/log.deflate"));
        CompressionInputStream cis = codec.createInputStream(new FileInputStream("/Users/wenhao/tmp/log.deflate"));
        FileOutputStream fos = new FileOutputStream("/Users/wenhao/tmp/1.log");
        IOUtils.copy(cis,fos,1024);
        fos.close();
        cis.close();
        System.out.println("over!!");

    }
}
