package com.dtner.hbase.base.create;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Collection;

/**
 * @ClassName CreateTable
 * @Description: 创建 Hbase 表
 * @Author dt
 * @Date 19-12-20
 **/
public class CreateTableUtils {

    /**
     * 创建 Hbase 表结构
     * @param con
     * @param tableName
     * @param familyDescriptors
     * @throws IOException
     */
    public static void CreateTable(Connection con, TableName tableName, Collection<ColumnFamilyDescriptor> familyDescriptors) throws IOException {
        Admin admin = con.getAdmin();
        /*表如果不存在，则创建表*/
        if(!admin.tableExists(tableName)){
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamilies(familyDescriptors).build();
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }

}
