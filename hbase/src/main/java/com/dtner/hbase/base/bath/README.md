## 批量操作
#### 1. 批量操作指令
1. 创建 Hbase 连接 Connection
2. 通过 Connection 获得 Table 对象
3. 使用 Table 的 bath api 同时操作多条指令

注意：执行顺序是不定的。所以需要格外注意。