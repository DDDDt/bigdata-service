package com.dtner.hbase.advance.filter;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName RandomRowFilterData
 * @Description: 随机行过滤器
 * @Author dt
 * @Date 19-12-31
 **/
public class RandomRowFilterData {

    /**
     * 在过滤器内部会使用 java 方法 Ｒandom.nextFloat() 来决定一行是否保被过滤, 使用这个方法的结果会与用户设定的
     *  chance 进行比较。如果用户为 chance 赋一个负值会导致所有结果都被过滤调，相反地，如果 chance 大于 1.0 则
     *  结果集中包含所有行.
     * @throws IOException
     */
    @Test
    public void randomRowFilterData() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();

        String tableName = "user_test_bath";
        Table table = con.getTable(TableName.valueOf(tableName));

        RandomRowFilter randomRowFilter = new RandomRowFilter(0.8F);
        Scan scan = new Scan()
                .setFilter(randomRowFilter);

        ResultScanner resultScanner = table.getScanner(scan);

        resultScanner.forEach(x -> System.out.println(x.toString()));

        resultScanner.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
