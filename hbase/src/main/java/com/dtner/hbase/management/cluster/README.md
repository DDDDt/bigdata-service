## 集群管理

1. 获取 hbase 连接 con
2. 通过 hbase 连接 con 得到 admin
3. 通过 admin 得到 ClusterMetrics
4. 通过 ClusterMetrics 得到集群的相关指标信息
5. 关闭 admin
6. 关闭 con