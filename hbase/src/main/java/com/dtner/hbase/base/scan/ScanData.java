package com.dtner.hbase.base.scan;

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
 * @ClassName ScanData
 * @Description: 扫描数据
 * @Author dt
 * @Date 19-12-26
 **/
public class ScanData {

    @Test
    public void scanData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();


        String tableName = "user_test";
        Table table = con.getTable(TableName.valueOf(tableName));

        /*设置上限和下限，并包含在内*/
        Scan scan = new Scan().withStartRow(Bytes.toBytes("bath_put_1"))
                .withStopRow(Bytes.toBytes("bath_put_2_1576812436916"),true);

        ResultScanner resultScan = table.getScanner(scan);
        resultScan.forEach(x -> {
            System.out.println(x.toString());
        });
        resultScan.close();

        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("bath-value-1-1")));
        Scan scanFilter = new Scan().setFilter(rowFilter);
        ResultScanner resultScanFilter = table.getScanner(scanFilter);
        resultScanFilter.forEach(x -> {
            System.out.println(x.toString());
        });
        resultScanFilter.close();
        
        ConnectionHbaseUtils.closeCon(con);

    }

}
