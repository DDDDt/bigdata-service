package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName RowFilterScan
 * @Description: 行过滤器
 * @Author dt
 * @Date 19-12-27
 **/
public class RowFilterData {

    /**
     * 使用 row filter
     * @throws IOException
     */
    @Test
    public void rowFilterScan() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        /*针对单个值判断过滤*/
        RowFilter rowFilter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("bath-value-1-1")));
        Scan scan = new Scan()
                .addColumn(Bytes.toBytes("dtner-family-1"), Bytes.toBytes("bath-qualifier-1-1"))
                .setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(x -> System.out.println(x.toString()));

        scanner.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
