package utils;

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
}
