package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName InclusiveStopFilterData
 * @Description: 包含结束的过滤器
 * @Author dt
 * @Date 19-12-30
 **/
public class InclusiveStopFilterData {

    public void inclusiveStopFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        new InclusiveStopFilter(Bytes.toBytes(""))

        ConnectionHbaseUtils.closeCon(con);

    }

}
