package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName InclusiveStopFilterData
 * @Description: 包含结束的过滤器
 * @Author dt
 * @Date 19-12-30
 **/
public class InclusiveStopFilterData {

    /**
     *  包含结束的过滤器
     * @throws IOException
     */
    @Test
    public void inclusiveStopFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        InclusiveStopFilter inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes("bath_put_2_1576823205294"));
        Scan scan = new Scan()
                .setFilter(inclusiveStopFilter);

        ResultScanner resultScanner = table.getScanner(scan);
        resultScanner.forEach(x -> System.out.println(x.toString()));
        resultScanner.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
