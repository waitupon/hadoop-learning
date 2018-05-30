package utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2018/5/29 0029.
 */
public class LogUtil {

    public static String getLog(String msg,int hashCode){
        try {
            String hostname =  InetAddress.getLocalHost().toString();
            long time = System.nanoTime();
            return "[" +hostname+"]." + hashCode +  "." + time  +"." + msg;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        LogUtil util = new LogUtil();
        System.out.println(util.hashCode()&Integer.MAX_VALUE);
        System.out.println(util.hashCode());
    }

}
