package com.dtner.hbase.base.crud;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName DeleteData
 * @Description: 删除 hbase 数据
 * @Author dt
 * @Date 19-12-25
 **/
public class DeleteData {

    /**
     * 删除单个数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        String rowKey = "bath_put_1_1576823205294";
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);

        ConnectionHbaseUtils.closeCon(con);

    }

    /**
     * 批量删除 hbase 数据
     * @throws IOException
     */
    @Test
    public void multipleDelete() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        Delete delete1 = new Delete(Bytes.toBytes("bath_put_2_1576820580571"));
        Delete delete2 = new Delete(Bytes.toBytes("bath_put_2_1576820499610"));
        List<Delete> deleteList = new ArrayList<>();
        deleteList.add(delete1);
        deleteList.add(delete2);

        table.delete(deleteList);

        ConnectionHbaseUtils.closeCon(con);

    }


    /**
     *  cas 满足条件才删除
     * @throws IOException
     */
    @Test
    public void checkDelete() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        String rowKey = "bath_put_1_1576820580570";
        String family = "dtner-family-2";
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        boolean delBol = table.checkAndMutate(Bytes.toBytes(rowKey), Bytes.toBytes(family))
                .qualifier(Bytes.toBytes("bath-qualifier-1-3"))
                .ifEquals(Bytes.toBytes("bath-value-1-3"))
                .thenDelete(delete);

        System.out.println(delBol);

        ConnectionHbaseUtils.closeCon(con);


    }



}
