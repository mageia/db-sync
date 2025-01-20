# DB-Sync

DB-Sync 是一个用于数据库备份和恢复的命令行工具，支持 PostgreSQL（MySQL 支持即将推出）。

## 功能特点

- 支持数据库级别和表级别的备份/恢复
- 支持分批处理大数据量
- 事务保证，确保数据一致性
- 详细的操作日志
- 支持选择性恢复和清空恢复
- 支持自定义批处理大小

## 安装

```bash
go install github.com/auto-tunnel/db-sync@latest
```

## 使用方法

### 命令行参数

```bash
Usage of db-sync:
  -op string
        操作类型: 
        - sync: 备份数据库到文件
        - load: 从文件恢复数据库
  -type string
        数据库类型: 
        - postgres: PostgreSQL 数据库
        - mysql: MySQL 数据库 (即将支持)
  -dsn string
        数据库连接字符串
  -file string
        备份文件路径
  -batch-size int
        批处理大小 (默认 1000)
  -tables string
        要处理的表（逗号分隔，为空则处理所有表）
  -clear
        恢复时是否清空目标表 (默认 false)
```

### 数据库连接字符串 (DSN) 格式

#### PostgreSQL
```
postgres://username:password@localhost:5432/dbname?sslmode=disable
```

参数说明：
- username: 数据库用户名
- password: 数据库密码
- localhost: 数据库主机地址
- 5432: 数据库端口
- dbname: 数据库名称
- sslmode: SSL 连接模式

### 使用示例

1. 备份整个数据库：
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql
```

2. 备份指定表：
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -tables "table1,table2,table3"
```

3. 恢复整个数据库：
```bash
db-sync -op load \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql
```

4. 恢复指定表（清空已存在的表）：
```bash
db-sync -op load \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -tables "table1,table2" \
    -clear
```

5. 使用自定义批处理大小：
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -batch-size 5000
```

## 注意事项

1. 备份和恢复操作都使用事务进行，确保数据一致性
2. 恢复时使用 `-clear` 参数会删除目标表中的所有数据
3. 建议在进行备份/恢复操作前先备份重要数据
4. 大数据量操作时可以调整 `-batch-size` 参数优化性能
5. 确保数据库用户具有足够的权限进行备份和恢复操作
6. 建议在低峰期进行大规模数据备份/恢复操作

## 限制

1. 目前仅支持 PostgreSQL 数据库
2. 不支持并发备份/恢复
3. 不支持增量备份
4. 不支持压缩和加密
5. 不支持跨版本数据库迁移
6. 不支持存储过程和触发器的备份/恢复

## 开发计划

- [ ] 添加 MySQL 支持
- [ ] 添加并发处理功能
- [ ] 支持备份文件压缩
- [ ] 支持增量备份
- [ ] 添加进度显示
- [ ] 支持更多数据库类型
- [ ] 添加数据校验功能
- [ ] 支持备份文件加密

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 作者

[作者名称]

## 联系方式

- GitHub Issues: [项目 Issues 页面]
- Email: [联系邮箱]

