package utils;

import org.junit.Test;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Created by Administrator on 2018/7/13 0013.
 */
public class TestNIO {

    @Test
    public void testSelector(){
        try {

            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(false);
            Selector selector = Selector.open();





        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
