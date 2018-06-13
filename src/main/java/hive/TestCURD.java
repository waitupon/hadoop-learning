package hive;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by Administrator on 2018/6/13 0013.
 */
public class TestCURD {
    private Connection conn;

    @Before
    public void initConn() throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        conn = DriverManager.getConnection("jdbc:hive2://192.168.3.61:10000/hive1","","");
        System.out.println("connect suceess");
    }

    @Test
    public void testSelect() throws Exception {
        PreparedStatement ppst = conn.prepareStatement("SELECT * from myuser");
        ResultSet rs = ppst.executeQuery();
        while(rs.next()){
            int id = rs.getInt("id");
            String name = rs.getString("name");
            System.out.println(id +"," + name);
        }
        rs.close();
        ppst.close();
        conn.close();
    }


}
