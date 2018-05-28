package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

public class MyServer {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Server server = new RPC.Builder(conf).setProtocol(IHelloWorld.class)
                    .setInstance(new HelloWorldImpl())
                    .setBindAddress("localhost")
                    .setNumHandlers(2)
                    .setPort(8888)
                    .build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
