package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName ColumnCountGetFilter
 * @Description: 列计数过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class ColumnCountGetFilterData {

    /**
     * 用来返回每行最多取回多少列
     * @throws IOException
     */
    @Test
    public void columnCountGetFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(1);
        Scan scan = new Scan()
                .setFilter(columnCountGetFilter);

        ResultScanner resultScanner = table.getScanner(scan);

        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();

        ConnectionHbaseUtils.closeCon(con);
    }

}
