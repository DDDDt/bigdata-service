# 计数器
## hbase shell
incr 增加　　

get_counter 得到值　　

get 得到的值是字节编码

### java client
- 获取 hbase 连接 con
- 通过 con 获得 table
- 使用 table 的 incrementColumnValue 或者使用 Increment
- 关闭连接

注意: hbase 仅支持单个 row key 上的多个计数器。如果不同的 row key 则需使用 bath