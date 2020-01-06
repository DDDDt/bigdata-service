package com.dtner.hbase.management.table;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName TableManagement
 * @Description: hbase 表结构的管理
 * @Author dt
 * @Date 20-1-3
 **/
public class TableManagement {

    /**
     * create hbase table
     * @throws IOException
     */
    @Test
    public void createHbaseTable() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        Admin admin = con.getAdmin();

        /*hbase table name*/
        TableName tableName = TableName.valueOf("user_table_admin");

        /*判断表是否已经存在*/
        if(admin.tableExists(tableName)) return;

        /*column family 列族*/
        List<ColumnFamilyDescriptor> familyDescriptorList = new ArrayList<>();
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_1"));
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_2"));
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_3"));

        /*表的描述信息，很多表的属性修改都可以在这进行修改*/
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamilies(familyDescriptorList)
                /*该表是否为只读*/
                .setReadOnly(true)
                .build();

                admin.createTable(tableDescriptor);

                /*检查表是否可用*/
        boolean tableAvailable = admin.isTableAvailable(tableName);
        System.out.println(tableAvailable);

        admin.close();
                
        ConnectionHbaseUtils.closeCon(con);

    }

    /**
     * modify hbase table
     * 修改表结构
     * @throws IOException
     */
    @Test
    public void modifyHbaseTable() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        Admin admin = con.getAdmin();

        TableName tableName = TableName.valueOf("user_table_admin");

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .removeColumnFamily(Bytes.toBytes("table_admin_family_1"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("table_admin_family_6"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of("table_admin_family_7"))
                .build();

        admin.disableTable(tableName);
        admin.modifyTable(tableDescriptor);
        admin.enableTable(tableName);

        TableDescriptor descriptor = admin.getDescriptor(tableName);
        System.out.println(descriptor.toStringCustomizedValues());

        admin.close();

        ConnectionHbaseUtils.closeCon(con);

    }

    /**
     * disable and delete hbase table
     * @throws IOException
     */
    @Test
    public void deleteHbaseTable() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        Admin admin = con.getAdmin();

        TableName tableName = TableName.valueOf("user_table_admin");

        // 删除 hbase table 时, 先 disable hbase table
        admin.disableTable(tableName);

        // delete hbase table
        admin.deleteTable(tableName);

        admin.close();
        ConnectionHbaseUtils.closeCon(con);

    }



}
