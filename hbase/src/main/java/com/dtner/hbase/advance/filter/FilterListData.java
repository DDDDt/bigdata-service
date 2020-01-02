package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ClassName FilterListData
 * @Description:  组合多个过滤器
 * @Author dt
 * @Date 20-1-2
 **/
public class FilterListData {

    /**
     * filter list 多个过滤器
     * MUST_PASS_ALL：当所有过滤器都允许这个值时，这个值才会被包含在结果中，也就是说没有过滤器会忽略这个值
     * MUST_PASS_ONE：只要有一个过滤器允许包括这个值，那这个值就会包含在结果中
     * @throws IOException
     */
    @Test
    public void filterListData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        RowFilter greaterRowFilter = new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("bath_put_1_1576820499610")));

        RowFilter lessRowFilter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("bath_put_2_1576823205294")));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, greaterRowFilter, lessRowFilter);
        Scan scan = new Scan()
                .setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);

        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();

        ConnectionHbaseUtils.closeCon(con);

    }

}
