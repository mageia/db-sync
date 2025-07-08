# DB-Sync

[![Go Version](https://img.shields.io/badge/Go-1.23+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/release/auto-tunnel/db-sync.svg)](https://github.com/auto-tunnel/db-sync/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/auto-tunnel/db-sync)](https://goreportcard.com/report/github.com/auto-tunnel/db-sync)

DB-Sync 是一个高性能的数据库备份和恢复命令行工具，支持 PostgreSQL 和 MySQL 数据库。

## ✨ 功能特点

- 🎯 **多数据库支持**: 支持 PostgreSQL 和 MySQL 数据库
- 📊 **灵活备份**: 支持数据库级别和表级别的备份/恢复
- 🚀 **高性能**: 支持分批处理大数据量，可自定义批处理大小
- 🔒 **数据安全**: 事务保证，确保数据一致性
- 📝 **详细日志**: 完整的操作日志记录
- 🎛️ **灵活选项**: 支持选择性恢复和清空恢复
- 🛡️ **优雅停机**: 支持信号处理和优雅停机

## 📦 安装

### 方式一：使用 Go 安装

```bash
go install github.com/auto-tunnel/db-sync@latest
```

### 方式二：下载预编译二进制文件

从 [Releases](https://github.com/auto-tunnel/db-sync/releases) 页面下载适合您系统的二进制文件。

### 方式三：使用包管理器

#### Ubuntu/Debian
```bash
# 下载 deb 包
wget https://github.com/auto-tunnel/db-sync/releases/latest/download/db-sync_linux_amd64.deb
sudo dpkg -i db-sync_linux_amd64.deb
```

#### CentOS/RHEL/Fedora
```bash
# 下载 rpm 包
wget https://github.com/auto-tunnel/db-sync/releases/latest/download/db-sync_linux_amd64.rpm
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
  -type string
        数据库类型: 
        - postgres: PostgreSQL 数据库
        - mysql: MySQL 数据库
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

1. **备份 MySQL 数据库：**
```bash
db-sync -op sync \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4" \
    -file backup.sql
```

2. **恢复 MySQL 数据库（清空已存在的表）：**
```bash
db-sync -op load \
    -type mysql \
    -dsn "user:pass@tcp(localhost:3306)/dbname?charset=utf8mb4" \
    -file backup.sql \
    -tables "table1,table2" \
    -clear
```

#### 高级选项

1. **使用自定义批处理大小：**
```bash
db-sync -op sync \
    -type postgres \
    -dsn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -file backup.sql \
    -batch-size 5000
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
- [ ] 🔄 添加并发处理功能
- [ ] 🔄 支持备份文件压缩
- [ ] 🔄 支持增量备份
- [ ] 🔄 添加进度显示
- [ ] 🔄 支持更多数据库类型 (SQLite, MongoDB)
- [ ] 🔄 添加数据校验功能
- [ ] 🔄 支持备份文件加密

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 开发环境设置

```bash
# 克隆仓库
git clone https://github.com/auto-tunnel/db-sync.git
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

[auto-tunnel](https://github.com/auto-tunnel)

## 📞 联系方式

- **GitHub Issues**: [项目 Issues 页面](https://github.com/auto-tunnel/db-sync/issues)
- **Discussions**: [项目讨论页面](https://github.com/auto-tunnel/db-sync/discussions)

---

**⭐ 如果这个项目对你有帮助，请给它一个 star！**

