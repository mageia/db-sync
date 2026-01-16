package main

import (
	"context"
	"db-sync/pkg/backup"
	"db-sync/pkg/backup/mysql"
	"db-sync/pkg/backup/postgres"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// CLILogger 实现 backup.Logger 接口
type CLILogger struct{}

func (l *CLILogger) Info(msg string, fields ...interface{}) {
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

// 全局变量
var (
	logger = &CLILogger{}
	ctx    context.Context
	cancel context.CancelFunc
)

// 脱敏 DSN 中的密码信息
func sanitizeDSN(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if u, err := url.Parse(dsn); err == nil {
			if u.User != nil {
				username := u.User.Username()
				u.User = url.User(username)
			}
			return u.String()
		}
	}
	if strings.HasPrefix(dsn, "mysql://") {
		if u, err := url.Parse(dsn); err == nil {
			if u.User != nil {
				username := u.User.Username()
				u.User = url.User(username)
			}
			return u.String()
		}
	}
	re := regexp.MustCompile(`(:)([^:@]+)(@)`)
	return re.ReplaceAllString(dsn, "${1}***${3}")
}

// 从 DSN 推测数据库类型
func detectDBType(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return "postgres"
	}
	if strings.Contains(dsn, "postgres") || strings.Contains(dsn, "postgresql") {
		return "postgres"
	}
	if strings.HasPrefix(dsn, "mysql://") {
		return "mysql"
	}
	if strings.Contains(dsn, "@tcp(") || strings.Contains(dsn, "@unix(") {
		return "mysql"
	}
	return ""
}

// 获取数据库实例
func getDBInstance(dsn string) (backup.DatabaseBackup, error) {
	dbType := detectDBType(dsn)
	switch dbType {
	case "postgres":
		return postgres.NewPostgresBackup(logger), nil
	case "mysql":
		return mysql.NewMySQLBackup(logger), nil
	default:
		return nil, fmt.Errorf("无法从DSN自动识别数据库类型，请检查DSN格式")
	}
}

// 从 DSN 中提取数据库名称
func extractDBNameFromDSN(dsn string) string {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		if parts := strings.Split(dsn, "/"); len(parts) > 3 {
			dbName := strings.Split(parts[3], "?")[0]
			if dbName != "" {
				return dbName
			}
		}
	}
	for _, part := range strings.Fields(dsn) {
		if strings.HasPrefix(part, "dbname=") {
			return strings.TrimPrefix(part, "dbname=")
		}
	}
	return "backup"
}

// 解析表名列表
func parseTables(tables string) []string {
	if tables == "" {
		return nil
	}
	return strings.Split(tables, ",")
}

func main() {
	// 设置信号处理
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		select {
		case <-sigChan:
			fmt.Println("\n收到停机信号，正在优雅停机...")
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	// 创建根命令
	rootCmd := &cobra.Command{
		Use:   "db-sync",
		Short: "数据库备份、恢复与同步工具",
		Long: `db-sync 是一个支持 PostgreSQL 和 MySQL 的数据库工具，提供以下功能：
  - backup:   备份数据库到 SQL 文件
  - restore:  从 SQL 文件恢复数据库
  - sync:     在两个数据库之间同步数据
  - validate: 校验两个数据库的数据一致性`,
		Version: "1.0.0",
	}

	// 添加子命令
	rootCmd.AddCommand(newBackupCmd())
	rootCmd.AddCommand(newRestoreCmd())
	rootCmd.AddCommand(newSyncCmd())
	rootCmd.AddCommand(newValidateCmd())

	// 添加全局示例
	rootCmd.Example = `  # 备份数据库
  db-sync backup -s "postgres://user:pass@localhost:5432/mydb" -o backup.sql

  # 恢复数据库
  db-sync restore -s "postgres://user:pass@localhost:5432/mydb" -i backup.sql

  # 同步数据库
  db-sync sync -s "postgres://user:pass@source:5432/db" -t "postgres://user:pass@target:5432/db"

  # 校验数据一致性
  db-sync validate -s "postgres://user:pass@source:5432/db" -t "postgres://user:pass@target:5432/db"`

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// ==================== backup 子命令 ====================

func newBackupCmd() *cobra.Command {
	var (
		source    string
		output    string
		tables    string
		batchSize int
		validate  bool
	)

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "备份数据库到 SQL 文件",
		Long: `将数据库中的数据备份到 SQL 文件。

支持的数据库类型会从 DSN 自动识别：
  - PostgreSQL: postgres://user:pass@host:port/dbname
  - MySQL:      user:pass@tcp(host:port)/dbname`,
		Example: `  # 备份整个数据库
  db-sync backup -s "postgres://user:pass@localhost:5432/mydb"

  # 备份到指定文件
  db-sync backup -s "postgres://user:pass@localhost:5432/mydb" -o backup.sql

  # 只备份指定表
  db-sync backup -s "postgres://user:pass@localhost:5432/mydb" -T users,orders

  # 备份并验证完整性
  db-sync backup -s "postgres://user:pass@localhost:5432/mydb" --validate`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBackup(source, output, tables, batchSize, validate)
		},
	}

	cmd.Flags().StringVarP(&source, "source", "s", "", "数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&output, "output", "o", "", "输出文件路径 (默认自动生成)")
	cmd.Flags().StringVarP(&tables, "tables", "T", "", "要备份的表，逗号分隔 (默认全部)")
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "批处理大小")
	cmd.Flags().BoolVar(&validate, "validate", false, "备份后验证完整性")

	cmd.MarkFlagRequired("source")

	return cmd
}

func runBackup(source, output, tables string, batchSize int, validate bool) error {
	db, err := getDBInstance(source)
	if err != nil {
		return err
	}

	if output == "" {
		dbName := extractDBNameFromDSN(source)
		dbType := detectDBType(source)
		timestamp := time.Now().Format("20060102_150405")
		output = fmt.Sprintf("%s_%s_%s.sql", dbName, dbType, timestamp)
		logger.Info("使用自动生成的备份文件路径", "path", output)
	}

	progressCallback := func(tableName string, processed, total int64) {
		if total > 0 {
			percent := float64(processed) / float64(total) * 100
			fmt.Printf("\r备份表 %s: %d/%d (%.1f%%)   ", tableName, processed, total, percent)
		} else {
			fmt.Printf("\r备份表 %s: %d 条记录   ", tableName, processed)
		}
	}

	err = db.Sync(ctx, source, output, backup.BackupOptions{
		BatchSize:        batchSize,
		Tables:           parseTables(tables),
		Logger:           logger,
		ProgressCallback: progressCallback,
	})
	fmt.Println()

	if err != nil {
		return fmt.Errorf("备份失败: %v [DSN: %s]", err, sanitizeDSN(source))
	}

	fmt.Printf("备份完成: %s\n", output)

	if validate {
		fmt.Println("正在验证备份完整性...")
		results, err := db.ValidateBackup(ctx, source, output, parseTables(tables))
		if err != nil {
			return fmt.Errorf("验证失败: %v", err)
		}

		allValid := true
		for _, result := range results {
			if result.IsValid {
				fmt.Printf("  ✓ 表 %s: 验证通过 (%d 条记录)\n", result.TableName, result.SourceCount)
			} else {
				fmt.Printf("  ✗ 表 %s: 验证失败 - %s\n", result.TableName, result.ErrorMessage)
				allValid = false
			}
		}

		if !allValid {
			return fmt.Errorf("备份验证失败，存在数据不一致问题")
		}
		fmt.Println("所有表验证通过")
	}

	return nil
}

// ==================== restore 子命令 ====================

func newRestoreCmd() *cobra.Command {
	var (
		target    string
		input     string
		tables    string
		batchSize int
		clear     bool
	)

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "从 SQL 文件恢复数据库",
		Long: `从 SQL 备份文件恢复数据到数据库。

注意：恢复操作会修改目标数据库，请确保已做好备份。`,
		Example: `  # 恢复数据库
  db-sync restore -t "postgres://user:pass@localhost:5432/mydb" -i backup.sql

  # 恢复前清空目标表
  db-sync restore -t "postgres://user:pass@localhost:5432/mydb" -i backup.sql --clear

  # 只恢复指定表
  db-sync restore -t "postgres://user:pass@localhost:5432/mydb" -i backup.sql -T users,orders`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runRestore(target, input, tables, batchSize, clear)
		},
	}

	cmd.Flags().StringVarP(&target, "target", "t", "", "目标数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&input, "input", "i", "", "备份文件路径 (必需)")
	cmd.Flags().StringVarP(&tables, "tables", "T", "", "要恢复的表，逗号分隔 (默认全部)")
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "批处理大小")
	cmd.Flags().BoolVar(&clear, "clear", false, "恢复前清空目标表")

	cmd.MarkFlagRequired("target")
	cmd.MarkFlagRequired("input")

	return cmd
}

func runRestore(target, input, tables string, batchSize int, clear bool) error {
	db, err := getDBInstance(target)
	if err != nil {
		return err
	}

	progressCallback := func(tableName string, processed, total int64) {
		if total > 0 {
			percent := float64(processed) / float64(total) * 100
			fmt.Printf("\r恢复表 %s: %d/%d (%.1f%%)   ", tableName, processed, total, percent)
		} else {
			fmt.Printf("\r恢复表 %s: %d 条记录   ", tableName, processed)
		}
	}

	err = db.Load(ctx, input, target, backup.LoadOptions{
		BatchSize:        batchSize,
		Tables:           parseTables(tables),
		ClearBeforeLoad:  clear,
		Logger:           logger,
		ProgressCallback: progressCallback,
	})
	fmt.Println()

	if err != nil {
		return fmt.Errorf("恢复失败: %v", err)
	}

	fmt.Println("恢复完成")
	return nil
}

// ==================== sync 子命令 ====================

func newSyncCmd() *cobra.Command {
	var (
		source           string
		target           string
		tables           string
		batchSize        int
		mode             string
		conflict         string
		timestampCol     string
		lastSyncTime     string
		dryRun           bool
		autoFixSchema    bool
		validateAfter    bool
	)

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "在两个数据库之间同步数据",
		Long: `将源数据库的数据同步到目标数据库。

支持两种同步模式：
  - full:        全量同步，清空目标表后重新导入所有数据
  - incremental: 增量同步，只同步指定时间之后变更的数据

注意：源和目标必须是相同类型的数据库（都是 PostgreSQL 或都是 MySQL）。`,
		Example: `  # 全量同步
  db-sync sync -s "postgres://user:pass@source:5432/db" -t "postgres://user:pass@target:5432/db"

  # 只同步指定表
  db-sync sync -s "postgres://..." -t "postgres://..." -T users,orders

  # 增量同步（需指定时间戳列）
  db-sync sync -s "postgres://..." -t "postgres://..." --mode incremental --timestamp-col updated_at

  # 试运行（不实际修改数据）
  db-sync sync -s "postgres://..." -t "postgres://..." --dry-run

  # 同步后验证数据一致性
  db-sync sync -s "postgres://..." -t "postgres://..." --validate`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSync(source, target, tables, batchSize, mode, conflict, 
				timestampCol, lastSyncTime, dryRun, autoFixSchema, validateAfter)
		},
	}

	cmd.Flags().StringVarP(&source, "source", "s", "", "源数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&target, "target", "t", "", "目标数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&tables, "tables", "T", "", "要同步的表，逗号分隔 (默认全部)")
	cmd.Flags().IntVarP(&batchSize, "batch-size", "b", 1000, "批处理大小")
	cmd.Flags().StringVar(&mode, "mode", "full", "同步模式: full(全量) 或 incremental(增量)")
	cmd.Flags().StringVar(&conflict, "conflict", "overwrite", "冲突策略: skip(跳过), overwrite(覆盖), fail(报错)")
	cmd.Flags().StringVar(&timestampCol, "timestamp-col", "", "增量同步的时间戳列名")
	cmd.Flags().StringVar(&lastSyncTime, "last-sync", "", "上次同步时间 (RFC3339格式，如 2024-01-01T00:00:00Z)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "试运行模式，不实际修改数据")
	cmd.Flags().BoolVar(&autoFixSchema, "auto-fix-schema", true, "自动修复目标表结构差异")
	cmd.Flags().BoolVar(&validateAfter, "validate", false, "同步后验证数据一致性")

	cmd.MarkFlagRequired("source")
	cmd.MarkFlagRequired("target")

	return cmd
}

func runSync(source, target, tables string, batchSize int, mode, conflict, 
	timestampCol, lastSyncTimeStr string, dryRun, autoFixSchema, validateAfter bool) error {
	
	// 检查数据库类型一致性
	sourceType := detectDBType(source)
	targetType := detectDBType(target)
	if sourceType == "" {
		return fmt.Errorf("无法识别源数据库类型，请检查 DSN 格式")
	}
	if targetType == "" {
		return fmt.Errorf("无法识别目标数据库类型，请检查 DSN 格式")
	}
	if sourceType != targetType {
		return fmt.Errorf("暂不支持跨数据库类型同步: 源=%s, 目标=%s", sourceType, targetType)
	}

	db, err := getDBInstance(source)
	if err != nil {
		return err
	}

	// 解析同步模式
	var syncMode backup.SyncMode
	switch mode {
	case "full":
		syncMode = backup.SyncModeFull
	case "incremental":
		syncMode = backup.SyncModeIncremental
		if timestampCol == "" {
			return fmt.Errorf("增量同步模式必须指定 --timestamp-col 参数")
		}
	default:
		return fmt.Errorf("不支持的同步模式: %s (可选: full, incremental)", mode)
	}

	// 解析冲突策略
	var strategy backup.ConflictStrategy
	switch conflict {
	case "skip":
		strategy = backup.ConflictStrategySkip
	case "overwrite":
		strategy = backup.ConflictStrategyOverwrite
	case "fail":
		strategy = backup.ConflictStrategyFail
	default:
		return fmt.Errorf("不支持的冲突策略: %s (可选: skip, overwrite, fail)", conflict)
	}

	// 解析上次同步时间
	var lastSync *time.Time
	if lastSyncTimeStr != "" {
		t, err := time.Parse(time.RFC3339, lastSyncTimeStr)
		if err != nil {
			return fmt.Errorf("无效的时间格式: %s (请使用 RFC3339 格式)", lastSyncTimeStr)
		}
		lastSync = &t
	}

	// 进度回调
	var lastTable string
	progressCallback := func(tableName string, processed, total int64) {
		if tableName != lastTable {
			if lastTable != "" {
				fmt.Println()
			}
			lastTable = tableName
		}
		if total > 0 {
			percent := float64(processed) / float64(total) * 100
			fmt.Printf("\r同步表 %s: %d/%d (%.1f%%)   ", tableName, processed, total, percent)
		}
		if processed == total && total > 0 {
			fmt.Println()
		}
	}

	// 执行同步
	results, err := db.SyncDatabase(ctx, source, target, backup.SyncOptions{
		Mode:             syncMode,
		Tables:           parseTables(tables),
		BatchSize:        batchSize,
		ConflictStrategy: strategy,
		TimestampColumn:  timestampCol,
		LastSyncTime:     lastSync,
		DryRun:           dryRun,
		AutoFixSchema:    autoFixSchema,
		Logger:           logger,
		ProgressCallback: progressCallback,
	})
	fmt.Println()

	if err != nil {
		return fmt.Errorf("同步失败: %v", err)
	}

	// 显示结果
	printSyncResults(results, syncMode)

	// 同步后验证
	if validateAfter && !dryRun {
		return runValidateAfterSync(db, source, target, tables)
	}

	return nil
}

// ==================== validate 子命令 ====================

func newValidateCmd() *cobra.Command {
	var (
		source        string
		target        string
		tables        string
		sampleSize    int
		compareDetail bool
	)

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "校验两个数据库的数据一致性",
		Long: `比较源数据库和目标数据库的数据，检查是否一致。

校验内容包括：
  - 表的行数是否相同
  - 数据校验和是否匹配
  - 可选：抽样比较具体数据

注意：源和目标必须是相同类型的数据库。`,
		Example: `  # 基本校验
  db-sync validate -s "postgres://user:pass@source:5432/db" -t "postgres://user:pass@target:5432/db"

  # 只校验指定表
  db-sync validate -s "postgres://..." -t "postgres://..." -T users,orders

  # 全量校验（不抽样）
  db-sync validate -s "postgres://..." -t "postgres://..." --sample-size 0

  # 显示详细差异
  db-sync validate -s "postgres://..." -t "postgres://..." --detail`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runValidate(source, target, tables, sampleSize, compareDetail)
		},
	}

	cmd.Flags().StringVarP(&source, "source", "s", "", "源数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&target, "target", "t", "", "目标数据库连接字符串 (必需)")
	cmd.Flags().StringVarP(&tables, "tables", "T", "", "要校验的表，逗号分隔 (默认全部)")
	cmd.Flags().IntVar(&sampleSize, "sample-size", 100, "抽样校验的记录数 (0=全量校验)")
	cmd.Flags().BoolVar(&compareDetail, "detail", false, "显示详细的差异信息")

	cmd.MarkFlagRequired("source")
	cmd.MarkFlagRequired("target")

	return cmd
}

func runValidate(source, target, tables string, sampleSize int, compareDetail bool) error {
	// 检查数据库类型一致性
	sourceType := detectDBType(source)
	targetType := detectDBType(target)
	if sourceType == "" {
		return fmt.Errorf("无法识别源数据库类型，请检查 DSN 格式")
	}
	if targetType == "" {
		return fmt.Errorf("无法识别目标数据库类型，请检查 DSN 格式")
	}
	if sourceType != targetType {
		return fmt.Errorf("暂不支持跨数据库类型校验: 源=%s, 目标=%s", sourceType, targetType)
	}

	db, err := getDBInstance(source)
	if err != nil {
		return err
	}

	progressCallback := func(tableName string, processed, total int64) {
		if total > 0 {
			percent := float64(processed) / float64(total) * 100
			fmt.Printf("\r校验进度: %d/%d 表 (%.1f%%)   ", processed, total, percent)
		}
	}

	fmt.Println("开始数据校验...")
	results, err := db.ValidateData(ctx, source, target, backup.DataValidationOptions{
		Tables:           parseTables(tables),
		SampleSize:       sampleSize,
		CompareData:      compareDetail,
		Logger:           logger,
		ProgressCallback: progressCallback,
	})
	fmt.Println()

	if err != nil {
		return fmt.Errorf("校验失败: %v", err)
	}

	// 显示结果
	printValidationResults(results, compareDetail)
	return nil
}

// ==================== 辅助函数 ====================

func printSyncResults(results []backup.SyncResult, mode backup.SyncMode) {
	fmt.Println()
	fmt.Println("=== 同步结果 ===")

	var totalProcessed, totalInserted, totalUpdated, totalSkipped int64
	allSuccess := true

	for _, r := range results {
		if r.ErrorMessage != "" {
			fmt.Printf("  ✗ %s: 失败 - %s\n", r.TableName, r.ErrorMessage)
			allSuccess = false
		} else {
			if mode == backup.SyncModeFull {
				fmt.Printf("  ✓ %s: %d 条记录 (耗时 %v)\n", r.TableName, r.InsertedRows, r.Duration)
			} else {
				fmt.Printf("  ✓ %s: 新增 %d, 更新 %d, 跳过 %d (耗时 %v)\n",
					r.TableName, r.InsertedRows, r.UpdatedRows, r.SkippedRows, r.Duration)
			}
		}
		totalProcessed += r.ProcessedRows
		totalInserted += r.InsertedRows
		totalUpdated += r.UpdatedRows
		totalSkipped += r.SkippedRows
	}

	fmt.Println()
	fmt.Printf("总计: 处理 %d 条, 新增 %d 条", totalProcessed, totalInserted)
	if mode == backup.SyncModeIncremental {
		fmt.Printf(", 更新 %d 条, 跳过 %d 条", totalUpdated, totalSkipped)
	}
	fmt.Println()

	if allSuccess {
		fmt.Println("同步完成")
	} else {
		fmt.Println("同步完成，但部分表存在错误")
	}
}

func runValidateAfterSync(db backup.DatabaseBackup, source, target, tables string) error {
	fmt.Println()
	fmt.Println("=== 同步后数据校验 ===")

	results, err := db.ValidateData(ctx, source, target, backup.DataValidationOptions{
		Tables:     parseTables(tables),
		SampleSize: 100,
		Logger:     logger,
	})
	if err != nil {
		return fmt.Errorf("校验失败: %v", err)
	}

	validCount := 0
	for _, r := range results {
		if r.IsValid {
			fmt.Printf("  ✓ %s: 数据一致 (%d 行)\n", r.TableName, r.SourceRows)
			validCount++
		} else {
			fmt.Printf("  ✗ %s: %s\n", r.TableName, r.ErrorMessage)
		}
	}

	fmt.Printf("\n校验结果: %d/%d 表通过\n", validCount, len(results))
	if validCount < len(results) {
		return fmt.Errorf("数据校验发现不一致")
	}
	return nil
}

func printValidationResults(results []backup.DataValidationResult, showDetail bool) {
	fmt.Println()
	fmt.Println("=== 校验结果 ===")

	validCount := 0
	for _, r := range results {
		if r.IsValid {
			fmt.Printf("  ✓ %s: 数据一致 (%d 行)\n", r.TableName, r.SourceRows)
			validCount++
		} else {
			fmt.Printf("  ✗ %s: %s\n", r.TableName, r.ErrorMessage)
			if r.SourceRows > 0 || r.TargetRows > 0 {
				fmt.Printf("      源: %d 行, 目标: %d 行\n", r.SourceRows, r.TargetRows)
			}
			if showDetail && len(r.Details) > 0 {
				displayCount := len(r.Details)
				if displayCount > 5 {
					displayCount = 5
				}
				fmt.Printf("      差异详情 (显示前 %d 条):\n", displayCount)
				for i := 0; i < displayCount; i++ {
					d := r.Details[i]
					fmt.Printf("        - 主键=%v, 列=%s, 源=%v, 目标=%v\n",
						d.PrimaryKey, d.ColumnName, d.SourceValue, d.TargetValue)
				}
				if len(r.Details) > 5 {
					fmt.Printf("        ... 还有 %d 条差异\n", len(r.Details)-5)
				}
			}
		}
	}

	fmt.Println()
	fmt.Printf("总计: %d/%d 表通过校验\n", validCount, len(results))
	if validCount == len(results) {
		fmt.Println("所有表数据一致")
	} else {
		fmt.Println("存在数据不一致，请检查上述输出")
	}
}
