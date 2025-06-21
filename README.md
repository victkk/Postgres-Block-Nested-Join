# 复旦大学数据库期末PJ-块嵌套循环连接(BNLJ)实现

## 项目概述

本项目成功修改了PostgreSQL的nodeNestloop.c执行器节点，实现了基于块的嵌套循环连接(Block Nested Loop Join)算法。相比标准嵌套循环连接(NLJ)，BNLJ通过分块处理元组，显著提升了查询性能。

## 核心特性

- 完全重写了PostgreSQL的嵌套循环连接执行器
- 支持可配置的块大小(block size)参数
- 保持了内连接(inner join)功能的正确性

## 实现原理

BNLJ算法的核心思想是将外部和内部关系分块处理：
- 将外部关系R分成多个块
- 对于每个外部块，读取内部关系S的多个块进行连接
- 通过减少内部关系的扫描次数和提高缓存命中率来优化性能

## 性能分析

随着块大小的增加，查询执行时间显著降低：

| 块大小 | 执行时间(ms) |
|--------|-------------|
| 1      | 926.332     |
| 2      | 686.763     |
| 4      | 419.336     |
| 8      | 304.016     |
| 16     | 267.488     |
| 32     | 232.112     |
| 64     | 216.764     |
| 128    | 204.482     |
| 256    | 192.263     |
| 512    | 185.356     |

性能提升主要源于：
1. 减少了内部关系的重复扫描次数
2. 提高了CPU缓存的命中率，使连接操作更高效

## 使用方法

要强制PostgreSQL使用BNLJ算法并配置参数：

```sql
-- 禁用其他可能的连接算法
SET enable_hashjoin = false;
SET enable_mergejoin = false;
SET enable_material = false;

-- 限制工作内存以模拟内存受限环境
SET work_mem = '64kB';

-- 执行查询并分析执行计划
EXPLAIN ANALYZE SELECT count(*) FROM table1 t1, table2 t2 WHERE t1.id = t2.id;
```

## 开发提示

编译和重启PostgreSQL服务器：
```bash
make && make install && $HOME/pgsql/bin/pg_ctl stop -D $HOME/pgsql/data && $HOME/pgsql/bin/pg_ctl start -D $HOME/pgsql/data && $HOME/pgsql/bin/psql postgres
```

## 完整代码

完整代码可在以下GitHub仓库中获取：
https://github.com/victkk/Postgres-Block-Nested-Join.git

## 关键提交
最小可运行的原始NestLoopJoin见提交855b2a6a，移除了对嵌套连接和外连接的支持使得代码看起来更清晰
BlockNestedLoopJoin实现见提交ce9df714
命令行文字提示和BlockSize设置实现见提交d9e56c7f和其前一个提交
## 致谢

特别感谢PKXX1943的blockSize命令行配置代码。
