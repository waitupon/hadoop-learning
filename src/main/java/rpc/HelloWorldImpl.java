package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class HelloWorldImpl implements IHelloWorld {
    public String sayHello(String msg) {
        System.out.println(msg);
        return msg;
    }

    public long getProtocolVersion(String protocal, long version) throws IOException {
        return IHelloWorld.versionID;
    }

    public ProtocolSignature getProtocolSignature(String protocal, long version, int clentMethodsHash) throws IOException {
        try {
            return ProtocolSignature.getProtocolSignature(protocal,version);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
