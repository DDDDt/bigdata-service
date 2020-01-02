package com.dtner.hbase.advance.counter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName MutlpieCounterData
 * @Description: 多计数器
 * @Author dt
 * @Date 20-1-2
 **/
public class MultipleCounterData {

    /**
     * 多个计数器
     * @throws IOException
     */
    @Test
    public void multipleCountData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "incr_table_test";
        Table table = con.getTable(TableName.valueOf(tableName));

        Increment increment = new Increment(Bytes.toBytes("202001021126"));
        increment.addColumn(Bytes.toBytes("incr_family1"),Bytes.toBytes("hits"),1L);
        increment.addColumn(Bytes.toBytes("incr_family2"),Bytes.toBytes("hits"),1L);

        Result result = table.increment(increment);

        for (Cell cell : result.rawCells()) {
            System.out.println(String.format("family = %s,qualifier = %s,value = %s",Bytes.toString(cell.getFamilyArray()),
                    Bytes.toString(cell.getQualifierArray()),
                    Bytes.toString(cell.getValueArray())));
        }

        ConnectionHbaseUtils.closeCon(con);

    }

}
