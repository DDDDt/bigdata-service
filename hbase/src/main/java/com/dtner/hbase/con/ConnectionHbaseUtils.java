package com.dtner.hbase.con;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName ConnectionHbase
 * @Description: 连接 Hbase
 * @Author dt
 * @Date 19-12-25
 **/
public class ConnectionHbaseUtils {

    /**
     * 连接 Hbase, 相关配置信息放在 配置文件中
     * @return
     * @throws IOException
     */
    public static Connection getCon() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        return ConnectionFactory.createConnection(conf);

    }

    /**
     * 关闭 Hbase 连接
     * @param con
     * @throws IOException
     */
    public static void closeCon(Connection con) throws IOException {

        con.close();

    }

}
