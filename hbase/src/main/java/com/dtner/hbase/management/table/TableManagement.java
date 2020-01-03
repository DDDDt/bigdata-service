package com.dtner.hbase.management.table;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

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

    public void tableManagement() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        Admin admin = con.getAdmin();

        /*hbase table name*/
        TableName tableName = TableName.valueOf("user_table_admin");

        /*column family 列族*/
        List<ColumnFamilyDescriptor> familyDescriptorList = new ArrayList<>();
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_1"));
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_2"));
        familyDescriptorList.add(ColumnFamilyDescriptorBuilder.of("table_admin_family_3"));


        /*表的描述信息，很多表的属性修改都可以在这进行修改*/
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamilies(familyDescriptorList)
                /*拆分block的阈值*/
                .setMaxFileSize(256000)
                /*该表是否为只读*/
                .setReadOnly(true)
                .build();

                admin.createTable(tableDescriptor);

                admin.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
