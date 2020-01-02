## 连接 Hbase 

### Hbase Connection
1. 通过 ConnectionFactory 得到 Connection
2. 在新版本中弃用了 HTablePool
3. 最好保证全局使用一个 Connection
