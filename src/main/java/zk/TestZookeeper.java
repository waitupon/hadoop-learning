package zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/12 0012.
 */
public class TestZookeeper {

    @Test
    public void createNode() throws Exception {
        String connectString = "localhost:2181";
        ZooKeeper zk = new ZooKeeper(connectString, 50 * 100, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("回调成功");
            }
        });

        zk.create("/root","hellroot".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    @Test
    public void getNode() throws Exception {
        String connectString = "localhost:2181";
        ZooKeeper zk = new ZooKeeper(connectString, 50 * 100, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("回调成功");
            }
        });

        byte[] data = zk.getData("/root", false, new Stat());
        System.out.println(new String(data));
    }

}
