package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName PageFilterData
 * @Description: 分页过滤器
 * @Author dt
 * @Date 19-12-30
 **/
public class PageFilterData {

    /**
     * 分页过滤器
     * @throws IOException
     */
    @Test
    public void pageFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        PageFilter pageFilter = new PageFilter(3);
        Scan scan = new Scan()
                .setFilter(pageFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
