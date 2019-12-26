package com.dtner.hbase.base.bath;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HbaseBatch
 * @Description: 批量操作 Hbase
 * @Author dt
 * @Date 19-12-26
 **/
public class HbaseBath {

    /**
     * 批量操作
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void bathOperation() throws IOException, InterruptedException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(Bytes.toBytes(tableName)));

        // Column Family
        String firstFamily = "dtner-family-1";
        String secondFaily = "dtner-family-2";

        Put put = new Put(Bytes.toBytes("bath_put_1_" + System.currentTimeMillis()));
        put.addColumn(Bytes.toBytes(firstFamily),Bytes.toBytes("bath-qualifier-1-1"),Bytes.toBytes("bath-value-1-1"));
        put.addColumn(Bytes.toBytes(secondFaily),Bytes.toBytes("bath-qualifier-2-1"),Bytes.toBytes("bath-value-2-1"));

        Get get = new Get(Bytes.toBytes("bath_put_1_1576820499610"));

        Delete delete = new Delete(Bytes.toBytes("bath_put_1_1576820459780"));

        List<Row> rowList = new ArrayList<>();
        rowList.add(put);
        rowList.add(get);
        rowList.add(delete);

        Object[] results = new Object[rowList.size()];
        table.batch(rowList,results);

        for (Object obj : results){
            System.out.println(obj.toString());
        }

        ConnectionHbaseUtils.closeCon(con);

    }

}
