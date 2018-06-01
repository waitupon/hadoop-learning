package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;

public class DBUtil {
    public static void setDBCondfig(Configuration conf){
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.6.14:3306/dwh","xiang","nEw-TESt@&2#");
    }
}
