package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName FamilyFilter
 * @Description:  Family filter
 * @Author dt
 * @Date 19-12-27
 **/
public class FamilyFilterData {

    @Test
    public void familyFilter() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("dtner-family-1")));
        
        Scan scan = new Scan()
                .setFilter(familyFilter);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(x -> System.out.println(x.toString()));
        scanner.close();


        System.out.println("get value ...");
        Get get = new Get(Bytes.toBytes("bath_put_1_1576820499610"));
        get.setFilter(familyFilter);
        Result result = table.get(get);
        System.out.println(result.toString());

        ConnectionHbaseUtils.closeCon(con);

    }

}
