package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName ColumnPaginationFilterData
 * @Description: 列分页过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class ColumnPaginationFilterData {

    /**
     * 列分页过滤器，与 PageFilter 相似，这个过滤器可以对一行的所有列进行分页。
     * @throws IOException
     */
    @Test
    public void columnPaginationFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(1, 2);
        Scan scan = new Scan()
                .setFilter(columnPaginationFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
