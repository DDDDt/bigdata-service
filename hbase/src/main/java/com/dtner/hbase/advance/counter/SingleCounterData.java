package com.dtner.hbase.advance.counter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName SingleCounterData
 * @Description: 单计数器
 * @Author dt
 * @Date 20-1-2
 **/
public class SingleCounterData {

    /**
     * 计数器增长
     * @throws IOException
     */
    @Test
    public void singleCounterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "incr_table_test";
        Table table = con.getTable(TableName.valueOf(tableName));

        long incrNum = table.incrementColumnValue(Bytes.toBytes("202001021126"), Bytes.toBytes("incr_family1"), Bytes.toBytes("hits"), 1L);

        System.out.println("incrNum = "+incrNum);

        long walIncrNum = table.incrementColumnValue(Bytes.toBytes("202001021126"),
                Bytes.toBytes("incr_family1"), Bytes.toBytes("hits"), 1L, Durability.ASYNC_WAL);

        System.out.println("walIncrNum = "+walIncrNum);

        ConnectionHbaseUtils.closeCon(con);

    }

}
