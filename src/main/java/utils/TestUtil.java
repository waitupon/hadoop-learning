package utils;

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestUtil {
    public static void main(String[] args) throws InterruptedException {
        List<String> list = new ArrayList<String>();

        for(int i=0;i<100;i++){
            list.add("t"+i);
        }


        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
        System.out.println("done!");

        /*while(list.size()>10){
           final List<String> newList = list.subList(0,10);
           new Thread(new Runnable() {
               public void run() {
                   System.out.println(Thread.currentThread().getName() + "  start");
                   System.out.println("newList " + newList);
                   try {
                       Thread.sleep(1000);
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
               }
           }).start();
//            Thread.sleep(1000);
            newList.clear();
            System.out.println("List " + list);
        }*/

    }



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
