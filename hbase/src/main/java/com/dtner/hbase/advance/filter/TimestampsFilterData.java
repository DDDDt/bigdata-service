package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName TimestampsFilterData
 * @Description: 时间戳过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class TimestampsFilterData {

    /**
     * 当用户需要在扫描结果中对版本进行细粒度的控制时，这个过滤器可以满足需求。
     * @throws IOException
     */
    @Test
    public  void timestampsFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        List<Long> ts = new ArrayList<>();
        ts.add(1576820472802L);
        ts.add(1577327150676L);
        ts.add(1576820433082L);
        ts.add(1576823178285L);
        TimestampsFilter timestampsFilter = new TimestampsFilter(ts);
        Scan scan = new Scan()
                .setFilter(timestampsFilter);

        ResultScanner resultScanner = table.getScanner(scan);

        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();
        ConnectionHbaseUtils.closeCon(con);
    }

}
