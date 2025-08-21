# DB-Sync

[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/release/mageia/db-sync.svg)](https://github.com/mageia/db-sync/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/mageia/db-sync)](https://goreportcard.com/report/github.com/mageia/db-sync)

DB-Sync 是一个高性能的数据库备份和恢复命令行工具，支持 PostgreSQL 和 MySQL 数据库。

## ✨ 功能特点

- 🎯 **多数据库支持**: 支持 PostgreSQL 和 MySQL 数据库
- 📊 **灵活备份**: 支持数据库级别和表级别的备份/恢复
- 🚀 **高性能**: 支持分批处理大数据量，可自定义批处理大小
- 🔄 **数据库同步**: 支持跨数据库实例的全量和增量同步
- 🔗 **外键约束处理**: 智能处理外键依赖，自动重试解决约束冲突
- 🔒 **数据安全**: 事务保证，确保数据一致性
- 📝 **详细日志**: 完整的操作日志记录
- 🎛️ **灵活选项**: 支持选择性恢复和清空恢复
- 🛡️ **优雅停机**: 支持信号处理和优雅停机

## 📦 安装

### 方式一：使用 Go 安装

```bash
go install github.com/mageia/db-sync@latest
```

### 方式二：下载预编译二进制文件

从 [Releases](https://github.com/mageia/db-sync/releases) 页面下载适合您系统的二进制文件。

### 方式三：使用包管理器

#### Ubuntu/Debian
```bash
# 下载 deb 包
wget https://github.com/mageia/db-sync/releases/latest/download/db-sync_linux_amd64.deb
sudo dpkg -i db-sync_linux_amd64.deb
```

#### CentOS/RHEL/Fedora
```bash
# 下载 rpm 包
wget https://github.com/mageia/db-sync/releases/latest/download/db-sync_linux_amd64.rpm
sudo rpm -i db-sync_linux_amd64.rpm
```

## 🚀 使用方法

### 命令行参数

```bash
Usage of db-sync:
  -op string
        操作类型: 
        - sync: 备份数据库到文件
        - load: 从文件恢复数据库
        - sync-db: 数据库间同步
  -type string
        数据库类型: 
        - postgres: PostgreSQL 数据库
        - mysql: MySQL 数据库
  -dsn string
        数据库连接字符串 (用于备份/恢复)
  -source-dsn string
        源数据库连接字符串 (用于数据库同步)
  -target-dsn string
        目标数据库连接字符串 (用于数据库同步)
  -file string
        备份文件路径
  -batch-size int
        批处理大小 (默认 1000)
  -tables string
        要处理的表（逗号分隔，为空则处理所有表）
  -clear
        恢复时是否清空目标表 (默认 false)
  -sync-mode string
        同步模式: full/incremental (默认 full)
  -conflict-strategy string
        冲突处理策略: skip/overwrite/fail (默认 overwrite)
  -timestamp-column string
        增量同步时间戳列名
  -dry-run
        试运行模式，不实际修改数据
```

### 数据库连接字符串 (DSN) 格式

#### PostgreSQL
```
postgres://username:password@localhost:5432/dbname?sslmode=disable
```

#### MySQL
```
username:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local
```

#### 参数说明：
- **username**: 数据库用户名
- **password**: 数据库密码
- **localhost**: 数据库主机地址
- **5432/3306**: 数据库端口
- **dbname**: 数据库名称
- **sslmode**: SSL 连接模式 (PostgreSQL)
- **charset**: 字符集 (MySQL)

### 💡 使用示例

#### PostgreSQL 示例

1. **备份整个数据库：**
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql
```

2. **备份指定表：**
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -tables "table1,table2,table3"
```

3. **恢复整个数据库：**
```bash
db-sync -op load \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql
```

#### MySQL 示例

1. **备份整个数据库：**
```bash
db-sync -op sync \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local" \
    -file backup.sql
```

2. **备份指定表：**
```bash
db-sync -op sync \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local" \
    -file backup.sql \
    -tables "users,orders,products"
```

3. **恢复整个数据库：**
```bash
db-sync -op load \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local" \
    -file backup.sql
```

4. **恢复指定表（清空已存在的表）：**
```bash
db-sync -op load \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local" \
    -file backup.sql \
    -tables "users,orders" \
    -clear
```

5. **使用自定义批处理大小备份大数据量表：**
```bash
db-sync -op sync \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local" \
    -file backup.sql \
    -batch-size 10000
```

#### 高级选项

1. **PostgreSQL 使用自定义批处理大小：**
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -batch-size 5000
```

2. **MySQL 跨数据库同步（从一个数据库备份并恢复到另一个数据库）：**
```bash
# 从生产环境备份
db-sync -op sync \
    -type mysql \
    -dsn "user:pass@tcp(prod-server:3306)/prod_db?charset=utf8mb4&parseTime=True&loc=Local" \
    -file prod_backup.sql \
    -tables "users,orders,products"

# 恢复到测试环境
db-sync -op load \
    -type mysql \
    -dsn "user:pass@tcp(test-server:3306)/test_db?charset=utf8mb4&parseTime=True&loc=Local" \
    -file prod_backup.sql \
    -clear
```

3. **PostgreSQL 跨数据库同步：**
```bash
# 从源数据库备份
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@source-host:5432/source_db?sslmode=disable" \
    -file migration.sql

# 恢复到目标数据库
db-sync -op load \
    -type postgres \
    -dsn "postgres://user:pass@target-host:5432/target_db?sslmode=disable" \
    -file migration.sql
```

### 🔄 数据库同步示例

#### 全库同步（智能处理外键约束）

```bash
# MySQL 全库同步
db-sync -op sync-db \
    -source-dsn "user:pass@tcp(source:3306)/sourcedb" \
    -target-dsn "user:pass@tcp(target:3306)/targetdb" \
    -sync-mode full

# PostgreSQL 全库同步
db-sync -op sync-db \
    -source-dsn "postgres://user:pass@source:5432/sourcedb" \
    -target-dsn "postgres://user:pass@target:5432/targetdb" \
    -sync-mode full
```

#### 增量同步

```bash
# MySQL 增量同步（基于时间戳）
db-sync -op sync-db \
    -source-dsn "user:pass@tcp(source:3306)/sourcedb" \
    -target-dsn "user:pass@tcp(target:3306)/targetdb" \
    -sync-mode incremental \
    -timestamp-column updated_at \
    -last-sync-time "2024-01-01T00:00:00Z"

# PostgreSQL 增量同步
db-sync -op sync-db \
    -source-dsn "postgres://user:pass@source:5432/sourcedb" \
    -target-dsn "postgres://user:pass@target:5432/targetdb" \
    -sync-mode incremental \
    -timestamp-column modified_at
```

#### 选择性表同步

```bash
# 仅同步指定的表
db-sync -op sync-db \
    -source-dsn "user:pass@tcp(source:3306)/sourcedb" \
    -target-dsn "user:pass@tcp(target:3306)/targetdb" \
    -tables "users,orders,products" \
    -sync-mode full
```

## 🔗 外键约束处理

DB-Sync 具备智能的外键约束处理机制，在全库同步时自动处理表之间的依赖关系：

### 工作原理

1. **自动检测**: 识别外键约束错误
2. **智能重试**: 将失败的表加入重试队列，等待依赖表同步完成
3. **依赖解析**: 从错误信息中提取表依赖关系
4. **循环依赖处理**: 检测循环依赖并临时禁用外键检查
5. **重试限制**: 最多重试3次，避免无限循环

### 特性

- ✅ 自动处理复杂的表依赖关系
- ✅ 支持多层级的外键依赖
- ✅ 智能检测和处理循环依赖
- ✅ 详细的重试日志和进度报告
- ✅ MySQL 和 PostgreSQL 均支持

### 日志示例

```
[INFO] 同步表 orders [retry=0, mode=full]
[INFO] 表同步因外键约束失败，加入重试队列 [table=orders, retry_count=1, dependencies=users,products]
[INFO] 同步表 users [retry=0, mode=full]
[INFO] 表同步成功 [table=users, rows=1000]
[INFO] 同步表 orders [retry=1, mode=full]
[INFO] 表同步成功 [table=orders, rows=5000]
```

## ⚠️ 注意事项

1. **数据一致性**: 备份和恢复操作都使用事务进行，确保数据一致性
2. **数据清空**: 恢复时使用 `-clear` 参数会删除目标表中的所有数据
3. **备份重要数据**: 建议在进行备份/恢复操作前先备份重要数据
4. **性能优化**: 大数据量操作时可以调整 `-batch-size` 参数优化性能
5. **用户权限**: 确保数据库用户具有足够的权限进行备份和恢复操作
6. **操作时机**: 建议在低峰期进行大规模数据备份/恢复操作
7. **优雅停机**: 支持 Ctrl+C 优雅停机，不会中断正在执行的事务

## 📋 当前限制

- ❌ 不支持并发备份/恢复
- ❌ 不支持增量备份
- ❌ 不支持压缩和加密
- ❌ 不支持跨版本数据库迁移
- ❌ 不支持存储过程和触发器的备份/恢复

## 🗺️ 开发计划

- [x] ✅ 添加 MySQL 支持
- [x] ✅ 添加优雅停机功能
- [x] ✅ 支持数据库间同步（全量/增量）
- [x] ✅ 智能处理外键约束依赖
- [x] ✅ 添加进度显示
- [ ] 🔄 添加并发处理功能
- [ ] 🔄 支持备份文件压缩
- [ ] 🔄 支持更多数据库类型 (SQLite, MongoDB)
- [ ] 🔄 添加数据校验功能
- [ ] 🔄 支持备份文件加密

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/mageia/db-sync.git
cd db-sync

# 安装依赖
go mod download

# 构建
make build

# 运行测试
make test
```

## 📄 许可证

本项目基于 [MIT License](LICENSE) 开源。

## 👥 作者

[mageia](https://github.com/mageia)

## 📞 联系方式

- **GitHub Issues**: [项目 Issues 页面](https://github.com/mageia/db-sync/issues)
- **Discussions**: [项目讨论页面](https://github.com/mageia/db-sync/discussions)

---

**⭐ 如果这个项目对你有帮助，请给它一个 star！**

