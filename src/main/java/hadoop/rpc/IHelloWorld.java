package hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IHelloWorld extends VersionedProtocol {
        static final long versionID = 1;
        public String sayHello(String msg);
}
