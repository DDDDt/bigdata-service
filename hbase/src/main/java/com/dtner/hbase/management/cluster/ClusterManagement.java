package com.dtner.hbase.management.cluster;

import com.dtner.hbase.base.con.ConnectionHbaseUtils;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @ClassName ClusterManagement
 * @Description: 集群管理
 * @Author dt
 * @Date 20-1-6
 **/
public class ClusterManagement {

    /**
     * 获取集群的一些指标信息
     * @throws IOException
     */
    @Test
    public void clusterManagement() throws IOException {

        Connection con = ConnectionHbaseUtils.getCon();
        Admin admin = con.getAdmin();
        ClusterMetrics clusterMetrics = admin.getClusterMetrics();

        System.out.println("cluster id = "+clusterMetrics.getClusterId());
        System.out.println("hbase version = "+clusterMetrics.getHBaseVersion());

        admin.close();
        ConnectionHbaseUtils.closeCon(con);

    }

}
