package com.dtner.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: dt
 * @Description:　向 hbase 插入数据
 * @Date: Create in 19-12-18
 */
public class CreateData {

    public Connection getCon() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        return ConnectionFactory.createConnection(conf);

    }

    @Test
    public void testPut() throws IOException {

        Connection con = getCon();

        Admin admin = con.getAdmin();

        TableName tableName = TableName.valueOf("user_test");

        /*表如果不存在，则创建表*/
        if(!admin.tableExists(tableName)){
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).build();
            admin.createTable(tableDescriptor);
        }


        Table table = con.getTable(tableName);

        // rowkey
        String rowkey = "ths test rowkey"+System.currentTimeMillis();

        Put put = new Put(Bytes.toBytes(rowkey));
        // Column Family
        String familyCol = "coldam1";
        put.addColumn(Bytes.toBytes(familyCol),Bytes.toBytes("test1"),Bytes.toBytes(value1));
        put.addColumn(Bytes.toBytes(familyCol),Bytes.toBytes("test2"),Bytes.toBytes(value2));
        put.addColumn(Bytes.toBytes(familyCol),Bytes.toBytes("test2"),Bytes.toBytes(value3));
        table.put(put);
        closeCon(con);

    }

    public void closeCon(Connection con) throws IOException {
        con.close();
    }

}
