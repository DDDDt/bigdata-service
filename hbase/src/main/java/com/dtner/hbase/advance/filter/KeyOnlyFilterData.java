package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName KeyOnlyFilter
 * @Description: 行键过滤器
 * @Author dt
 * @Date 19-12-30
 **/
public class KeyOnlyFilterData {

    /**
     * 只返回 key 不返回 value
     * @throws IOException
     */
    @Test
    public void keyOnlyFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
        Scan scan = new Scan()
                .setFilter(keyOnlyFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));

        ConnectionHbaseUtils.closeCon(con);

    }

}
