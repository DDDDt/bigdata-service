package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName PrefixFilterData
 * @Description: 列前缀过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class ColumnPrefixFilterData {

    /**
     * 列前缀过滤器
     * @throws IOException
     */
    @Test
    public void columnPrefixFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("bath-qualifier-2"));
        Scan scan = new Scan()
                .setFilter(columnPrefixFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();

        ConnectionHbaseUtils.closeCon(con);

    }

}
