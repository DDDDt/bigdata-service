package com.dtner.hbase.base.crud;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * @ClassName GetData
 * @Description: 从Hbase 查询值
 * @Author dt
 * @Date 19-12-24
 **/
public class GetData {

    /**
     * 单次查询值
     * @throws IOException
     */
    @Test
    public void getValue() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        String family = "dtner-family-1";
        String quailfier = "bath-qualifier-1-1";

        /*通过 rowkey ,fanily 和 qualifier 精确查找*/
        Get rowGet = new Get(Bytes.toBytes("bath_put_1_1576820459780"));
        Result result = table.get(rowGet);
        byte[] valueByte = result.getValue(Bytes.toBytes(family), Bytes.toBytes(quailfier));
        System.out.println("值 = "+Bytes.toString(valueByte));

        /*只通过 family 和 quailfier 返回 map*/
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));

        familyMap.forEach((k,v) -> {
            System.out.println("k = "+Bytes.toString(k) + " ;v = "+Bytes.toString(v));
        });

        ConnectionHbaseUtils.closeCon(con);
    }

    /**
     * 同时查询多个 get
     */
    @Test
    public void multipleGet() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        Get get1 = new Get(Bytes.toBytes("bath_put_2_1576820459792"));
        Get get2 = new Get(Bytes.toBytes("bath_put_2_1576820499610"));
        List<Get> getList = new ArrayList<>();
        getList.add(get1);
        getList.add(get2);
        Result[] results = table.get(getList);

        for(Result result : results){
            System.out.println(result.toString());
        }

        ConnectionHbaseUtils.closeCon(con);

    }

}
