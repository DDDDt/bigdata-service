## hbase table 管理

1. 得到 hbase 的连接 con
2. 通过连接 con 得到 admin
3. 创间 ist<ColumnFamilyDescriptor> 列族的列表
4. 创建 TableDescriptor 并设置好相关的 hbase table 属性，set进列族.
5. admin 创建表
6. admin close
7. con close