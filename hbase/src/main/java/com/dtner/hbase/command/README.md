## Hbase 指令
### １. 创建表指令
create 'table_name','family'...

### 计数器
- 增加减少某个值: incr 'table_name','row_key','family:column_name',count_num
- 查询某个计数器的值: get_counter 'table_name','row_key','family:column'