##　Ｈbase 增删改查

### 创建 Hbase 表
1. 创建连接 Connection
2. 通过 Connection 获得 Admin
3. 通过 Admin 判断表是否存在
4. 不存在，通过 Admin 的方法创建

### 插入 Hbase 表数据

1. 创建连接 Connection
2. 通过 Connection 获得 Table 对象
3. 创建 Put
4. 通过 Table 对象将 Put 插入 Hbase 中

### 查询 Hbase 表数据
1. 创建连接 Connection
2. 通过 Connection 获得 Tbale 对象
3. 创建 Get
4. 通过 Table 对象的 get　方法查询值　　

注意: Hbase 新版本已经移除了 getRowOrBefore. 最好结合 es 来进行模糊查询。


