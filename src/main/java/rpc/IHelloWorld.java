package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface IHelloWorld extends VersionedProtocol {
        static final long versionID = 1;
        public String sayHello(String msg);
}
