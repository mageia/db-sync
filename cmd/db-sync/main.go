package main

import (
	"context"
	"db-sync/pkg/backup"
	"db-sync/pkg/backup/mysql"
	"db-sync/pkg/backup/postgres"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"net/url"
	"regexp"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
)

type CLILogger struct{}

func (l *CLILogger) Info(msg string, fields ...interface{}) {
	// 将 key-value 对转换为字符串
	var pairs []string
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			pairs = append(pairs, fmt.Sprintf("%v=%v", fields[i], fields[i+1]))
		}
	}

	if len(pairs) > 0 {
		log.Printf("[INFO] %s [%s]", msg, strings.Join(pairs, ", "))
	} else {
		log.Printf("[INFO] %s", msg)
	}
}

func (l *CLILogger) Error(msg string, fields ...interface{}) {
	var pairs []string
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			pairs = append(pairs, fmt.Sprintf("%v=%v", fields[i], fields[i+1]))
		}
	}

	if len(pairs) > 0 {
		log.Printf("[ERROR] %s [%s]", msg, strings.Join(pairs, ", "))
	} else {
		log.Printf("[ERROR] %s", msg)
	}
}

func (l *CLILogger) Debug(msg string, fields ...interface{}) {
	var pairs []string
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			pairs = append(pairs, fmt.Sprintf("%v=%v", fields[i], fields[i+1]))
		}
	}

	if len(pairs) > 0 {
		log.Printf("[DEBUG] %s [%s]", msg, strings.Join(pairs, ", "))
	} else {
		log.Printf("[DEBUG] %s", msg)
	}
}

// 脱敏 DSN 中的密码信息
func sanitizeDSN(dsn string) string {
	// PostgreSQL URL 格式
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			if u.User != nil {
				username := u.User.Username()
				u.User = url.User(username) // 只保留用户名，移除密码
			}
			return u.String()
		}
	}

	// MySQL URL 格式
	if strings.HasPrefix(dsn, "mysql://") {
		if u, err := url.Parse(dsn); err == nil {
			if u.User != nil {
				username := u.User.Username()
				u.User = url.User(username)
			}
			return u.String()
		}
	}

	// 其他格式（如 user:pass@tcp(host)/db）使用正则替换
	re := regexp.MustCompile(`(:)([^:@]+)(@)`)
	return re.ReplaceAllString(dsn, "${1}***${3}")
}

// 添加新的函数：从 DSN 推测数据库类型
func detectDBType(dsn string) string {
	// PostgreSQL URL 格式检测
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return "postgres"
	}

	// PostgreSQL key=value 格式检测
	if strings.Contains(dsn, "postgres") || strings.Contains(dsn, "postgresql") {
		return "postgres"
	}

	// MySQL URL 格式检测
	if strings.HasPrefix(dsn, "mysql://") {
		return "mysql"
	}

	// MySQL 标准格式检测 (user:pass@tcp(host:port)/dbname)
	if strings.Contains(dsn, "@tcp(") || strings.Contains(dsn, "@unix(") {
		return "mysql"
	}

	return ""
}

func main() {
	// 设置信号处理和优雅停机
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan) // 确保清理信号监听

	// 信号处理
	go func() {
		select {
		case <-sigChan:
			fmt.Println("\n收到停机信号，正在优雅停机...")
			cancel()
		case <-ctx.Done():
			// 正常完成或被取消，退出 goroutine
			return
		}
	}()

	var (
		operation   string
		dbType      string
		dsn         string
		file        string
		batchSize   int
		tables      string
		clearBefore bool
		validate    bool
	)

	// 定义参数，自动支持长短形式
	pflag.StringVarP(&operation, "op", "o", "", "操作类型: sync/load")
	pflag.StringVarP(&dbType, "type", "t", "", "数据库类型: postgres/mysql (可选，通常可以从DSN自动识别)")
	pflag.StringVarP(&dsn, "dsn", "d", "", "数据库连接字符串")
	pflag.StringVarP(&file, "file", "f", "", "备份文件路径")
	pflag.IntVarP(&batchSize, "batch-size", "b", 1000, "批处理大小")
	pflag.StringVarP(&tables, "tables", "T", "", "要处理的表（逗号分隔）")
	pflag.BoolVarP(&clearBefore, "clear", "C", false, "恢复前是否清空表")
	pflag.BoolVarP(&validate, "validate", "V", false, "备份后是否验证完整性")

	pflag.Parse()

	// 检查必需参数并提供友好的错误提示
	var missingFlags []string
	if operation == "" {
		missingFlags = append(missingFlags, "--op")
	} else if operation != "sync" && operation != "load" {
		fmt.Printf("错误: 不支持的操作类型 '%s'，必须是 sync 或 load\n", operation)
		os.Exit(1)
	}

	if dsn == "" {
		missingFlags = append(missingFlags, "--dsn")
	}

	if len(missingFlags) > 0 {
		fmt.Println("错误: 缺少必需的参数:")
		for _, flag := range missingFlags {
			fmt.Printf("  %s\n", flag)
		}
		fmt.Println("\n使用示例:")
		fmt.Printf("  备份数据库:   %s --op sync --dsn \"postgresql://user:***@host:port/dbname\"\n", os.Args[0])
		fmt.Printf("  恢复数据库:   %s --op load --dsn \"postgresql://user:***@host:port/dbname\" --file backup.sql\n", os.Args[0])
		fmt.Println("\n可用的参数:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// 自动检测数据库类型
	detectedType := detectDBType(dsn)
	if detectedType == "" && dbType == "" {
		fmt.Println("错误: 无法从DSN自动识别数据库类型，请使用 --type 参数指定数据库类型 (postgres/mysql)")
		os.Exit(1)
	}

	// 如果未指定类型，使用检测到的类型；如果指定了类型，验证其有效性
	finalDBType := detectedType
	if dbType != "" {
		if dbType != "postgres" && dbType != "mysql" {
			fmt.Printf("错误: 不支持的数据库类型 '%s'，必须是 postgres 或 mysql\n", dbType)
			os.Exit(1)
		}
		// 如果检测到的类型与指定的类型不一致，发出警告
		if detectedType != "" && detectedType != dbType {
			fmt.Printf("警告: DSN格式显示数据库类型为 %s，但用户指定为 %s\n", detectedType, dbType)
		}
		finalDBType = dbType
	}

	var db backup.DatabaseBackup
	logger := &CLILogger{}

	// 创建数据库实例
	switch finalDBType {
	case "postgres":
		db = postgres.NewPostgresBackup(logger)
	case "mysql":
		db = mysql.NewMySQLBackup(logger)
	default:
		fmt.Printf("错误: 不支持的数据库类型: %s\n", finalDBType)
		os.Exit(1)
	}

	// 使用带取消机制的上下文

	// 解析表名
	var tableList []string
	if tables != "" {
		tableList = strings.Split(tables, ",")
	}

	switch operation {
	case "sync":
		if file == "" {
			// 从 DSN 中提取数据库名称
			dbName := extractDBNameFromDSN(dsn)
			// 生成备份文件名：dbname_type_timestamp.sql
			timestamp := time.Now().Format("20060102_150405")
			file = fmt.Sprintf("%s_%s_%s.sql", dbName, finalDBType, timestamp)
			logger.Info("使用自动生成的备份文件路径", "path", file)
		}

		// 设置进度显示回调
		progressCallback := func(tableName string, processed, total int64) {
			if total > 0 {
				percent := float64(processed) / float64(total) * 100
				fmt.Printf("\r备份表 %s: %d/%d (%.1f%%)   ", tableName, processed, total, percent)
			} else {
				fmt.Printf("\r备份表 %s: %d 条记录   ", tableName, processed)
			}
		}

		err := db.Sync(ctx, dsn, file, backup.BackupOptions{
			BatchSize:        batchSize,
			Tables:           tableList,
			Logger:           logger,
			ProgressCallback: progressCallback,
		})
		fmt.Println() // 换行
		if err != nil {
			fmt.Printf("备份失败: %v [DSN: %s]\n", err, sanitizeDSN(dsn))
			os.Exit(1)
		}

		// 如果启用验证，执行备份完整性检查
		if validate {
			fmt.Println("正在验证备份完整性...")
			results, err := db.ValidateBackup(ctx, dsn, file, tableList)
			if err != nil {
				fmt.Printf("验证失败: %v\n", err)
				os.Exit(1)
			}

			allValid := true
			for _, result := range results {
				if result.IsValid {
					fmt.Printf("✓ 表 %s: 验证通过 (%d 条记录)\n", result.TableName, result.SourceCount)
				} else {
					fmt.Printf("✗ 表 %s: 验证失败 - %s\n", result.TableName, result.ErrorMessage)
					allValid = false
				}
			}

			if !allValid {
				fmt.Println("备份验证失败，存在数据不一致问题")
				os.Exit(1)
			}
			fmt.Println("所有表验证通过，备份完整性正常")
		}

	case "load":
		if file == "" {
			fmt.Println("需要指定备份文件路径")
			os.Exit(1)
		}

		// 设置进度显示回调
		progressCallback := func(tableName string, processed, total int64) {
			if total > 0 {
				percent := float64(processed) / float64(total) * 100
				fmt.Printf("\r恢复表 %s: %d/%d (%.1f%%)   ", tableName, processed, total, percent)
			} else {
				fmt.Printf("\r恢复表 %s: %d 条记录   ", tableName, processed)
			}
		}

		err := db.Load(ctx, file, dsn, backup.LoadOptions{
			BatchSize:        batchSize,
			Tables:           tableList,
			ClearBeforeLoad:  clearBefore,
			Logger:           logger,
			ProgressCallback: progressCallback,
		})
		fmt.Println() // 换行
		if err != nil {
			fmt.Printf("恢复失败: %v [DSN: %s]\n", err, sanitizeDSN(dsn))
			os.Exit(1)
		}

	default:
		fmt.Printf("不支持的操作类型: %s\n", operation)
		os.Exit(1)
	}

	// 检查是否被用户取消
	select {
	case <-ctx.Done():
		// 检查取消原因
		if ctx.Err() == context.Canceled {
			fmt.Println("操作被用户取消")
			os.Exit(130) // SIGINT 退出码
		}
	default:
		// 正常完成
	}

	fmt.Println("操作完成")
}

// 添加辅助函数：从 DSN 中提取数据库名称
func extractDBNameFromDSN(dsn string) string {
	// PostgreSQL DSN 格式：postgres://username:password@host:port/dbname?param=value
	// 或者：host=localhost port=5432 dbname=mydb user=postgres password=secret

	// 处理 URL 格式
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if parts := strings.Split(dsn, "/"); len(parts) > 3 {
			dbName := strings.Split(parts[3], "?")[0]
			if dbName != "" {
				return dbName
			}
		}
	}

	// 处理 key=value 格式
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			return strings.TrimPrefix(part, "dbname=")
		}
	}

	// 如果无法提取数据库名称，返回默认值
	return "backup"
}
