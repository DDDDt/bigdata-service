package com.dtner.hbase.crud;

import com.dtner.hbase.con.ConnectionHbaseUtils;
import com.dtner.hbase.create.CreateTableUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: dt
 * @Description:　向 hbase 插入数据
 * @Date: Create in 19-12-18
 */
public class CreateData {


    /**
     * 普通方式保存
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        Admin admin = con.getAdmin();

        TableName tableName = TableName.valueOf("user_test");

        // Column Family
       String firstFamily = "dtner-family-1";
       String secondFaily = "dtner-family-2";

        /*表如果不存在，则创建表*/
        List<ColumnFamilyDescriptor> familys = new ArrayList<>();
        familys.add(ColumnFamilyDescriptorBuilder.of(firstFamily));
        familys.add(ColumnFamilyDescriptorBuilder.of(secondFaily));
        CreateTableUtils.CreateTable(con,tableName,familys);

        Table table = con.getTable(tableName);

        // rowkey
        String rowkey = "ths test rowkey"+System.currentTimeMillis();
        // 保存数据
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("test-1-1"),Bytes.toBytes("value-1-1"));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("test-1-2"),Bytes.toBytes("value-1-2"));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("test-1-2"),Bytes.toBytes("value-1-3"));
        put.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("test-2-1"),Bytes.toBytes("value-2-1"));
        table.put(put);
        ConnectionHbaseUtils.closeCon(con);

    }

    /**
     * 批量插入数据到 hbase
     * 还可以通过 bath api 来使用
     * @throws IOException
     */
    @Test
    public void bathPut() throws Exception {

        Connection con = ConnectionHbaseUtils.getCon();
        TableName tableName = TableName.valueOf("user_test_bath");
        // Column Family
        String firstFamily = "dtner-family-1";
        String secondFaily = "dtner-family-2";

        /*列族*/
        List<ColumnFamilyDescriptor> list = new ArrayList<>();
        list.add(ColumnFamilyDescriptorBuilder.of(firstFamily));
        list.add(ColumnFamilyDescriptorBuilder.of(secondFaily));
        CreateTableUtils.CreateTable(con,tableName,list);

        Table table = con.getTable(tableName);

        List<Put> putList = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("bath_put_1_"+System.currentTimeMillis()));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("bath-qualifier-1-1"),Bytes.toBytes("bath-value-1-1"));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("bath-qualifier-1-2"),Bytes.toBytes("bath-value-1-2"));
        put.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("bath-qualifier-1-3"),Bytes.toBytes("bath-value-1-3"));
        putList.add(put);

        Put put1 = new Put(Bytes.toBytes("bath_put_2_" + System.currentTimeMillis()));
        put1.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("bath-qualifier-2-1"),Bytes.toBytes("bath-value-2-1"));
        put1.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("bath-qualifier-2-2"),Bytes.toBytes("bath-value-2-2"));
        put1.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("bath-qualifier-2-3"),Bytes.toBytes("bath-value-2-3"));
        putList.add(put1);

        // 保存集合 put
//        table.put(putList);

        Object[] results = new Object[putList.size()];
        table.batch(putList,results);

        System.out.println(results.toString());

        ConnectionHbaseUtils.closeCon(con);

    }


    /**
     * cas
     * @throws Exception
     */
    @Test
    public void casPut() throws Exception {

        Connection con = ConnectionHbaseUtils.getCon();
        TableName tableName = TableName.valueOf("user_test_cas");
        // Column Family
        String firstFamily = "dtner-family-1";
        String secondFaily = "dtner-family-2";

        /*列族*/
        List<ColumnFamilyDescriptor> list = new ArrayList<>();
        list.add(ColumnFamilyDescriptorBuilder.of(firstFamily));
        list.add(ColumnFamilyDescriptorBuilder.of(secondFaily));
        CreateTableUtils.CreateTable(con,tableName,list);

        Table table = con.getTable(tableName);

        Put put = new Put(Bytes.toBytes("cas_put_1"));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("cas-qualifier-1-1"),Bytes.toBytes("cas-value-1-1"));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("cas-qualifier-1-2"),Bytes.toBytes("cas-value-1-2"));
        put.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("cas-qualifier-1-3"),Bytes.toBytes("cas-value-1-3"));
        table.put(put);

        Put putUpdate = new Put(Bytes.toBytes("cas_put_1"));
        putUpdate.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("cas-qualifier-2-1"),Bytes.toBytes("cas-value-2-1"));
        putUpdate.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("cas-qualifier-2-2"),Bytes.toBytes("cas-value-2-2"));
        putUpdate.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("cas-qualifier-2-3"),Bytes.toBytes("cas-value-2-3"));

        // 如果不存在字段就插入
        table.checkAndMutate(Bytes.toBytes("cas_put_1"),Bytes.toBytes(firstFamily))
                .qualifier(Bytes.toBytes("cas-qualifier-1-1"))
                .ifNotExists()
                .thenPut(putUpdate);

        ConnectionHbaseUtils.closeCon(con);

    }


}
