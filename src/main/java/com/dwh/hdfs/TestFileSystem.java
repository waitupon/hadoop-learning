package com.dwh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by Administrator on 2018/5/16 0016.
 */
public class TestFileSystem {

    String hdfsPath = "hdfs://master:9000";

    Configuration conf = null;

    FileSystem fs = null;

    @Before
    public void initConf(){
        Configuration conf = new Configuration();
        try {
             fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWrite() throws Exception {
        FSDataOutputStream dos = fs.create(new Path(hdfsPath + "/usr/hello.txt"));
        dos.write("hello".getBytes());
        dos.close();
        System.out.println("over");

    }


    @Test
    public void testRead() throws Exception {
        FSDataInputStream dis = fs.open(new Path(hdfsPath + "/usr/hello.txt"));
        FileOutputStream fos = new FileOutputStream("d:/hello.txt");
        IOUtils.copyBytes(dis,fos,1024);
        IOUtils.closeStream(dis);
        IOUtils.closeStream(fos);
    }


    @Test
    public void testSeek() throws Exception {
        FSDataInputStream dis = fs.open(new Path(hdfsPath + "/usr/hello.txt"));
        FileOutputStream fos = new FileOutputStream("d:/hello.txt");
        dis.seek(2);
        IOUtils.copyBytes(dis,fos,1024);
        IOUtils.closeStream(dis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/usr/myfolder");
        boolean b = fs.mkdirs(path, FsPermission.getDefault());
        System.out.println(b);
    }


    @Test
    public void testStatus() throws Exception {
        FileStatus fileStatus = fs.getFileStatus(new Path("/"));
        Class clazz = fileStatus.getClass();
        Method[] methods = clazz.getMethods();
        for(Method m : methods){
            Class[] parameterTypes = m.getParameterTypes();
            if(m.getName().startsWith("get")  && (parameterTypes==null || parameterTypes.length ==0)){
                if(!m.getName().equals("getSymlink")){
                    Object ret = m.invoke(fileStatus, null);
                    System.out.println(m.getName() + "()=" + ret);
                }
            }
        }
    }


}
