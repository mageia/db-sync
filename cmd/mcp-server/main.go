package main

import (
	"context"
	"db-sync/pkg/backup"
	"db-sync/pkg/backup/mysql"
	"db-sync/pkg/backup/postgres"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type MCPLogger struct{}

func (l *MCPLogger) Info(msg string, fields ...interface{}) {
	log.Printf("[INFO] %s %v", msg, fields)
}

func (l *MCPLogger) Error(msg string, fields ...interface{}) {
	log.Printf("[ERROR] %s %v", msg, fields)
}

func (l *MCPLogger) Debug(msg string, fields ...interface{}) {
	log.Printf("[DEBUG] %s %v", msg, fields)
}

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

func getDBInstance(dsn string, logger backup.Logger) (backup.DatabaseBackup, error) {
	dbType := detectDBType(dsn)
	switch dbType {
	case "postgres":
		return postgres.NewPostgresBackup(logger), nil
	case "mysql":
		return mysql.NewMySQLBackup(logger), nil
	default:
		return nil, fmt.Errorf("无法从DSN自动识别数据库类型: %s", sanitizeDSN(dsn))
	}
}

func main() {
	s := server.NewMCPServer(
		"db-sync",
		"1.0.0",
		server.WithToolCapabilities(false),
	)

	// Tool 1: db_backup - 备份数据库
	backupTool := mcp.NewTool("db_backup",
		mcp.WithDescription("备份数据库到SQL文件"),
		mcp.WithString("dsn",
			mcp.Required(),
			mcp.Description("数据库连接字符串，如 postgresql://user:pass@host:5432/dbname 或 user:pass@tcp(host:3306)/dbname"),
		),
		mcp.WithString("output_file",
			mcp.Description("输出文件路径，不指定则自动生成"),
		),
		mcp.WithString("tables",
			mcp.Description("要备份的表名，逗号分隔，不指定则备份所有表"),
		),
		mcp.WithNumber("batch_size",
			mcp.Description("批处理大小，默认1000"),
		),
	)
	s.AddTool(backupTool, handleBackup)

	// Tool 2: db_restore - 恢复数据库
	restoreTool := mcp.NewTool("db_restore",
		mcp.WithDescription("从SQL备份文件恢复数据库"),
		mcp.WithString("dsn",
			mcp.Required(),
			mcp.Description("目标数据库连接字符串"),
		),
		mcp.WithString("backup_file",
			mcp.Required(),
			mcp.Description("备份文件路径"),
		),
		mcp.WithString("tables",
			mcp.Description("要恢复的表名，逗号分隔，不指定则恢复所有表"),
		),
		mcp.WithBoolean("clear_before",
			mcp.Description("恢复前是否清空表，默认false"),
		),
		mcp.WithNumber("batch_size",
			mcp.Description("批处理大小，默认1000"),
		),
	)
	s.AddTool(restoreTool, handleRestore)

	// Tool 3: db_sync - 数据库间同步
	syncTool := mcp.NewTool("db_sync",
		mcp.WithDescription("在两个数据库之间同步数据"),
		mcp.WithString("source_dsn",
			mcp.Required(),
			mcp.Description("源数据库连接字符串"),
		),
		mcp.WithString("target_dsn",
			mcp.Required(),
			mcp.Description("目标数据库连接字符串"),
		),
		mcp.WithString("tables",
			mcp.Description("要同步的表名，逗号分隔，不指定则同步所有表"),
		),
		mcp.WithString("sync_mode",
			mcp.Description("同步模式: full(全量) 或 incremental(增量)，默认full"),
		),
		mcp.WithString("conflict_strategy",
			mcp.Description("冲突处理策略: skip/overwrite/fail，默认overwrite"),
		),
		mcp.WithString("timestamp_column",
			mcp.Description("增量同步时间戳列名（增量模式必需）"),
		),
		mcp.WithString("last_sync_time",
			mcp.Description("上次同步时间，RFC3339格式"),
		),
		mcp.WithBoolean("dry_run",
			mcp.Description("试运行模式，不实际修改数据"),
		),
		mcp.WithNumber("batch_size",
			mcp.Description("批处理大小，默认1000"),
		),
	)
	s.AddTool(syncTool, handleSync)

	// Tool 4: db_validate - 数据校验
	validateTool := mcp.NewTool("db_validate",
		mcp.WithDescription("验证两个数据库之间的数据一致性"),
		mcp.WithString("source_dsn",
			mcp.Required(),
			mcp.Description("源数据库连接字符串"),
		),
		mcp.WithString("target_dsn",
			mcp.Required(),
			mcp.Description("目标数据库连接字符串"),
		),
		mcp.WithString("tables",
			mcp.Description("要校验的表名，逗号分隔，不指定则校验所有表"),
		),
		mcp.WithNumber("sample_size",
			mcp.Description("抽样校验的记录数，0表示全量校验，默认100"),
		),
		mcp.WithBoolean("compare_detail",
			mcp.Description("是否比较数据详情，默认false"),
		),
	)
	s.AddTool(validateTool, handleValidate)

	if err := server.ServeStdio(s); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}

func handleBackup(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dsn := mcp.ParseString(request, "dsn", "")
	if dsn == "" {
		return mcp.NewToolResultError("缺少必需参数 dsn"), nil
	}

	logger := &MCPLogger{}
	db, err := getDBInstance(dsn, logger)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	outputFile := mcp.ParseString(request, "output_file", "")
	if outputFile == "" {
		timestamp := time.Now().Format("20060102_150405")
		dbType := detectDBType(dsn)
		outputFile = fmt.Sprintf("backup_%s_%s.sql", dbType, timestamp)
	}

	var tableList []string
	tables := mcp.ParseString(request, "tables", "")
	if tables != "" {
		tableList = strings.Split(tables, ",")
	}

	batchSize := mcp.ParseInt(request, "batch_size", 1000)

	err = db.Sync(ctx, dsn, outputFile, backup.BackupOptions{
		BatchSize: batchSize,
		Tables:    tableList,
		Logger:    logger,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("备份失败: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("备份成功，文件保存至: %s", outputFile)), nil
}

func handleRestore(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dsn := mcp.ParseString(request, "dsn", "")
	if dsn == "" {
		return mcp.NewToolResultError("缺少必需参数 dsn"), nil
	}

	backupFile := mcp.ParseString(request, "backup_file", "")
	if backupFile == "" {
		return mcp.NewToolResultError("缺少必需参数 backup_file"), nil
	}

	logger := &MCPLogger{}
	db, err := getDBInstance(dsn, logger)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	var tableList []string
	tables := mcp.ParseString(request, "tables", "")
	if tables != "" {
		tableList = strings.Split(tables, ",")
	}

	clearBefore := mcp.ParseBoolean(request, "clear_before", false)
	batchSize := mcp.ParseInt(request, "batch_size", 1000)

	err = db.Load(ctx, backupFile, dsn, backup.LoadOptions{
		BatchSize:       batchSize,
		Tables:          tableList,
		ClearBeforeLoad: clearBefore,
		Logger:          logger,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("恢复失败: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("数据库恢复成功，从文件: %s", backupFile)), nil
}

func handleSync(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sourceDSN := mcp.ParseString(request, "source_dsn", "")
	if sourceDSN == "" {
		return mcp.NewToolResultError("缺少必需参数 source_dsn"), nil
	}

	targetDSN := mcp.ParseString(request, "target_dsn", "")
	if targetDSN == "" {
		return mcp.NewToolResultError("缺少必需参数 target_dsn"), nil
	}

	logger := &MCPLogger{}
	db, err := getDBInstance(sourceDSN, logger)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	var tableList []string
	tables := mcp.ParseString(request, "tables", "")
	if tables != "" {
		tableList = strings.Split(tables, ",")
	}

	mode := backup.SyncModeFull
	syncMode := mcp.ParseString(request, "sync_mode", "full")
	if syncMode == "incremental" {
		mode = backup.SyncModeIncremental
	}

	strategy := backup.ConflictStrategyOverwrite
	cs := mcp.ParseString(request, "conflict_strategy", "overwrite")
	switch cs {
	case "skip":
		strategy = backup.ConflictStrategySkip
	case "fail":
		strategy = backup.ConflictStrategyFail
	}

	timestampColumn := mcp.ParseString(request, "timestamp_column", "")
	if mode == backup.SyncModeIncremental && timestampColumn == "" {
		return mcp.NewToolResultError("增量同步模式必须指定 timestamp_column"), nil
	}

	var lastSync *time.Time
	lst := mcp.ParseString(request, "last_sync_time", "")
	if lst != "" {
		if parsedTime, err := time.Parse(time.RFC3339, lst); err == nil {
			lastSync = &parsedTime
		}
	}

	dryRun := mcp.ParseBoolean(request, "dry_run", false)
	batchSize := mcp.ParseInt(request, "batch_size", 1000)

	results, err := db.SyncDatabase(ctx, sourceDSN, targetDSN, backup.SyncOptions{
		Mode:             mode,
		Tables:           tableList,
		BatchSize:        batchSize,
		ConflictStrategy: strategy,
		TimestampColumn:  timestampColumn,
		LastSyncTime:     lastSync,
		DryRun:           dryRun,
		AutoFixSchema:    true,
		Logger:           logger,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("同步失败: %v", err)), nil
	}

	var sb strings.Builder
	sb.WriteString("同步完成:\n")
	var totalInserted, totalUpdated int64
	for _, r := range results {
		if r.ErrorMessage != "" {
			sb.WriteString(fmt.Sprintf("- 表 %s: 失败 - %s\n", r.TableName, r.ErrorMessage))
		} else {
			sb.WriteString(fmt.Sprintf("- 表 %s: 新增 %d, 更新 %d, 耗时 %v\n",
				r.TableName, r.InsertedRows, r.UpdatedRows, r.Duration))
			totalInserted += r.InsertedRows
			totalUpdated += r.UpdatedRows
		}
	}
	sb.WriteString(fmt.Sprintf("\n总计: 新增 %d 条, 更新 %d 条", totalInserted, totalUpdated))

	return mcp.NewToolResultText(sb.String()), nil
}

func handleValidate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sourceDSN := mcp.ParseString(request, "source_dsn", "")
	if sourceDSN == "" {
		return mcp.NewToolResultError("缺少必需参数 source_dsn"), nil
	}

	targetDSN := mcp.ParseString(request, "target_dsn", "")
	if targetDSN == "" {
		return mcp.NewToolResultError("缺少必需参数 target_dsn"), nil
	}

	logger := &MCPLogger{}
	db, err := getDBInstance(sourceDSN, logger)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	var tableList []string
	tables := mcp.ParseString(request, "tables", "")
	if tables != "" {
		tableList = strings.Split(tables, ",")
	}

	sampleSize := mcp.ParseInt(request, "sample_size", 100)
	compareDetail := mcp.ParseBoolean(request, "compare_detail", false)

	results, err := db.ValidateData(ctx, sourceDSN, targetDSN, backup.DataValidationOptions{
		Tables:      tableList,
		SampleSize:  sampleSize,
		CompareData: compareDetail,
		Logger:      logger,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("校验失败: %v", err)), nil
	}

	var sb strings.Builder
	sb.WriteString("数据校验结果:\n")
	validCount, invalidCount := 0, 0
	for _, r := range results {
		if r.IsValid {
			sb.WriteString(fmt.Sprintf("- 表 %s: 通过 (行数: %d)\n", r.TableName, r.SourceRows))
			validCount++
		} else {
			sb.WriteString(fmt.Sprintf("- 表 %s: 失败 - %s\n", r.TableName, r.ErrorMessage))
			invalidCount++
		}
	}
	sb.WriteString(fmt.Sprintf("\n总计: %d 个表通过, %d 个表失败", validCount, invalidCount))

	return mcp.NewToolResultText(sb.String()), nil
}
