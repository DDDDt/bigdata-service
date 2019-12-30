package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName FirstKeyOnlyFilter
 * @Description: 首次行键过滤器
 * @Author dt
 * @Date 19-12-30
 **/
public class FirstKeyOnlyFilterData {

    /**
     * 首次行键过滤器，对于一行中的只返回第一列
     * @throws IOException
     */
    @Test
    public void firstKeyOnlyFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        Scan scan = new Scan()
                .setFilter(firstKeyOnlyFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();

        ConnectionHbaseUtils.closeCon(con);

    }

}
