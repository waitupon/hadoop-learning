package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            IHelloWorld proxy = RPC.getProxy(IHelloWorld.class, IHelloWorld.versionID,
                    new InetSocketAddress("localhost", 8888), conf);

            String msg = proxy.sayHello("hello world");
            System.out.println(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
