package utils;

import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TestUtil {
    public static List<String> list = new ArrayList<String>();

    public static HashMap<String,Integer> map = new HashMap<String,Integer>();
    public static void main(String[] args) throws InterruptedException {
        TestUtil.showFileName("E:\\迅雷下载\\G-queen");
        for(String key : map.keySet()){
            System.out.println(key +  "     " + map.get(key));
        }
        System.out.println("一共完成" +list.size()  +"个文件");
    }



    public  static void showFileName(String path){
        File parentFile = new File(path);
        File[] files = parentFile.listFiles();

        if(files!=null && files.length >0){
            for(File childFile :files){
                if(childFile.isDirectory()){
                    showFileName(childFile.getAbsolutePath());
                }else{
                    if(!childFile.getName().contains("bt.xltd") && !childFile.getName().contains(".torrent")){
                      //  System.out.println(childFile.getAbsolutePath());
                        int i = childFile.getAbsolutePath().lastIndexOf("\\");
                        String dirPath = childFile.getAbsolutePath().substring(0,i);
                        if(map.containsKey(dirPath)){
                            Integer num = map.get(dirPath);
                            map.put(dirPath,++num);
                        }else{
                            map.put(dirPath,1);
                        }
                        list.add(childFile.getAbsolutePath());
                    }
                }
            }
        }

    }

//    private void getName(String path) {
//        File file = new File(path);
//        if(file.isDirectory()){
//            String[] list = file.list();
//        }
//    }


    @Test
    public void combineFile() throws Exception {
        FileInputStream fis1 = new FileInputStream("d://1.txt");
        BufferedReader bis1 = new BufferedReader(new InputStreamReader(fis1));

        FileInputStream fis2 = new FileInputStream("d://2.txt");
        BufferedReader bis2 = new BufferedReader(new InputStreamReader(fis2,"utf-8"));

        FileOutputStream fos = new FileOutputStream("d://3.txt");
        String s = null;
        while((s = bis1.readLine()) !=null){
                String str = s + "\t\t\t\t\t\t\t" + bis2.readLine() + "\r\n";
                fos.write(str.getBytes("utf-8"));
        }

        fos.close();
        bis2.close();
        fis2.close();

        bis1.close();
        bis2.close();

    }


}
