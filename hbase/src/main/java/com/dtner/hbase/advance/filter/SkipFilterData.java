package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName SkipFilterData
 * @Description: 跳转过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class SkipFilterData {

    /**
     * skip filter
     * 当过滤器发现某一行中的一列需要过滤时，那么整行数据都将被过滤掉
     * @throws IOException
     */
    @Test
    public void skipFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        /* value filter*/
        System.out.println("value filter....");
        ValueFilter valueFilter = new ValueFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("bath-value-1-1")));
        Scan scan = new Scan()
                .setFilter(valueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));
        resultScanner.close();

        System.out.println("skip filter......");
        SkipFilter skipFilter = new SkipFilter(valueFilter);
        Scan skipScan = scan.setFilter(skipFilter);
        ResultScanner skipResultScanner = table.getScanner(skipScan);
        skipResultScanner.forEach(x -> System.out.println(x.toString()));
        skipResultScanner.close();

        ConnectionHbaseUtils.closeCon(con);

    }

}
