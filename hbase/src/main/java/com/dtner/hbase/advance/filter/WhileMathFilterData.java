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
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName WhileMathFilterData
 * @Description: 全匹配过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class WhileMathFilterData {

    /**
     * 当第一条数据被过滤掉时，它就会直接放弃这次扫描操作
     * @throws IOException
     */
    @Test
    public void whileMathFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        System.out.println("row filter....");
        RowFilter rowFilter = new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("bath-qualifier-1-1")));
        Scan rowScan = new Scan()
                .setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(rowScan);
        resultScanner.forEach(x -> System.out.println(x.toString()));
        resultScanner.close();

        System.out.println("while math filter...");
        WhileMatchFilter whileMatchFilter = new WhileMatchFilter(rowFilter);
        rowScan.setFilter(whileMatchFilter);
        ResultScanner mathResultScanner = table.getScanner(rowScan);
        mathResultScanner.forEach(x -> System.out.println(x.toString()));
        mathResultScanner.close();

        ConnectionHbaseUtils.closeCon(con);

    }

}
