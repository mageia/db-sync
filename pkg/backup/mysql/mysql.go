package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"db-sync/pkg/backup"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLBackup struct {
	logger backup.Logger
}

// NewMySQLBackup 创建一个新的 MySQLBackup 实例
func NewMySQLBackup(logger backup.Logger) *MySQLBackup {
	return &MySQLBackup{
		logger: logger,
	}
}

func (m *MySQLBackup) Sync(ctx context.Context, dsn string, outputPath string, opts backup.BackupOptions) error {
	db, err := m.openDBWithRetry(ctx, dsn)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	defer db.Close()

	// 开启事务
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback()

	// 获取表列表
	tables := opts.Tables
	if len(tables) == 0 {
		tables, err = m.getAllTables(ctx, tx)
		if err != nil {
			return err
		}
	}

	// 创建输出文件并设置安全权限 (0600 - 仅所有者可读写)
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("创建备份文件失败: %w", err)
	}
	defer file.Close()

	// 写入文件头
	if _, err := file.WriteString("SET FOREIGN_KEY_CHECKS=0;\n\n"); err != nil {
		return fmt.Errorf("写入文件头失败: %w", err)
	}

	// 逐表备份
	for _, table := range tables {
		m.logger.Info("开始备份表", "table", table)

		if err := m.backupTable(ctx, tx, table, file, opts.BatchSize, opts.ProgressCallback); err != nil {
			return fmt.Errorf("备份表 %s 失败: %w", table, err)
		}
	}

	// 写入文件尾
	if _, err := file.WriteString("\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return fmt.Errorf("写入文件尾失败: %w", err)
	}

	return tx.Commit()
}

func (m *MySQLBackup) backupTable(ctx context.Context, tx *sql.Tx, table string, file *os.File, batchSize int, progressCallback backup.ProgressCallback) error {
	// 获取表结构
	schema, err := m.getTableSchema(ctx, tx, table)
	if err != nil {
		return err
	}

	// 写入表结构
	if _, err := fmt.Fprintf(file, "-- Table: %s\nDROP TABLE IF EXISTS `%s`;\n%s\n\n", table, table, schema); err != nil {
		return err
	}

	// 获取列名
	columns, err := m.getTableColumns(ctx, tx, table)
	if err != nil {
		return fmt.Errorf("获取表列信息失败: %w", err)
	}

	// 获取表总记录数用于进度显示
	var totalRows int64
	if progressCallback != nil {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		if err := tx.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
			m.logger.Debug("获取表记录数失败", "table", table, "error", err)
			totalRows = 0
		}
	}

	// 分批查询数据 - 使用参数化查询
	offset := 0
	var processedRows int64
	for {
		query := fmt.Sprintf("SELECT * FROM `%s` LIMIT ? OFFSET ?", table)
		rows, err := tx.QueryContext(ctx, query, batchSize, offset)
		if err != nil {
			return err
		}

		// 创建缓冲写入器以提高性能
		bufWriter := bufio.NewWriterSize(file, 64*1024) // 64KB 缓冲
		defer bufWriter.Flush()

		// 构建 INSERT 语句前缀 - 使用安全的标识符
		quotedColumns := make([]string, len(columns))
		for i, col := range columns {
			quotedColumns[i] = fmt.Sprintf("`%s`", col)
		}
		insertPrefix := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES ", table, strings.Join(quotedColumns, ", "))

		count := 0
		batchCount := 0
		var batchValues []string

		for rows.Next() {
			// 准备接收数据的切片
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			// 扫描数据
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("扫描数据失败: %w", err)
			}

			// 构建值列表
			vals := make([]string, len(columns))
			for i, val := range values {
				if val == nil {
					vals[i] = "NULL"
				} else {
					switch v := val.(type) {
					case []byte:
						vals[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "\\'"))
					case string:
						vals[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "\\'"))
					case time.Time:
						vals[i] = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
					case bool:
						if v {
							vals[i] = "1"
						} else {
							vals[i] = "0"
						}
					default:
						vals[i] = fmt.Sprintf("%v", v)
					}
				}
			}
			batchValues = append(batchValues, fmt.Sprintf("(%s)", strings.Join(vals, ", ")))
			batchCount++
			count++

			// 每100条记录或缓冲区快满时写入一次
			if batchCount >= 100 || bufWriter.Available() < 1024 {
				if err := m.writeBatch(bufWriter, insertPrefix, batchValues); err != nil {
					rows.Close()
					return err
				}
				batchValues = batchValues[:0] // 清空切片但保留容量
				batchCount = 0
				
				// 更新进度
				processedRows = int64(offset + count)
				if progressCallback != nil {
					progressCallback(table, processedRows, totalRows)
				}
			}
		}

		// 写入剩余的数据
		if batchCount > 0 {
			if err := m.writeBatch(bufWriter, insertPrefix, batchValues); err != nil {
				return err
			}
		}

		// 最终进度更新
		processedRows = int64(offset + count)
		if progressCallback != nil {
			progressCallback(table, processedRows, totalRows)
		}
		rows.Close()

		if count < batchSize {
			break
		}
		offset += batchSize
		m.logger.Debug("正在备份数据", "table", table, "offset", offset)
	}

	return nil
}

func (m *MySQLBackup) getAllTables(ctx context.Context, tx *sql.Tx) ([]string, error) {
	query := "SHOW TABLES"
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("查询表列表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("读取表名失败: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (m *MySQLBackup) getTableSchema(ctx context.Context, tx *sql.Tx, table string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", table)
	var tableName, createSQL string
	err := tx.QueryRowContext(ctx, query).Scan(&tableName, &createSQL)
	if err != nil {
		return "", fmt.Errorf("获取表结构失败: %w", err)
	}

	return createSQL, nil
}

func (m *MySQLBackup) getTableColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`", table)
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, type_, null, key, default_, extra sql.NullString
		if err := rows.Scan(&field, &type_, &null, &key, &default_, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, field.String)
	}

	return columns, nil
}

func (m *MySQLBackup) Load(ctx context.Context, backupFile string, dsn string, opts backup.LoadOptions) error {
	db, err := m.openDBWithRetry(ctx, dsn)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %w", err)
	}
	defer db.Close()

	// 读取备份文件
	content, err := os.ReadFile(backupFile)
	if err != nil {
		return fmt.Errorf("读取备份文件失败: %w", err)
	}

	// 开启事务
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback()

	// 禁用外键检查
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("禁用外键检查失败: %w", err)
	}

	// 按语句分割SQL文件
	statements := splitSQLStatements(string(content))

	for _, stmt := range statements {
		// 跳过注释和空行
		if isCommentOrEmpty(stmt) {
			continue
		}

		// 获取当前语句的表名
		tableName := extractTableName(stmt)
		if tableName != "" && len(opts.Tables) > 0 {
			// 如果指定了要恢复的表，则跳过不在列表中的表
			found := false
			for _, t := range opts.Tables {
				if t == tableName {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// 执行SQL语句
		m.logger.Info("执行SQL语句", "table", tableName)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("执行SQL语句失败: %w", err)
		}
	}

	// 启用外键检查
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1"); err != nil {
		return fmt.Errorf("启用外键检查失败: %w", err)
	}

	return tx.Commit()
}

// 辅助函数：分割SQL语句
func splitSQLStatements(content string) []string {
	var statements []string
	var currentStmt strings.Builder
	lines := strings.Split(content, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		currentStmt.WriteString(line)
		currentStmt.WriteString(" ")
		if strings.HasSuffix(line, ";") {
			statements = append(statements, strings.TrimSpace(currentStmt.String()))
			currentStmt.Reset()
		}
	}

	return statements
}

// 辅助函数：判断是否是注释或空行
func isCommentOrEmpty(stmt string) bool {
	stmt = strings.TrimSpace(stmt)
	return stmt == "" || strings.HasPrefix(stmt, "--")
}

// 辅助函数：从CREATE TABLE语句中提取表名
func extractTableName(stmt string) string {
	stmt = strings.TrimSpace(strings.ToUpper(stmt))
	if strings.HasPrefix(stmt, "CREATE TABLE") {
		parts := strings.Fields(stmt)
		if len(parts) >= 3 {
			tableName := strings.Trim(parts[2], "`")
			return strings.Trim(tableName, `"`)
		}
	}
	return ""
}

// writeBatch 批量写入 INSERT 语句
func (m *MySQLBackup) writeBatch(writer *bufio.Writer, insertPrefix string, values []string) error {
	if len(values) == 0 {
		return nil
	}
	
	sql := insertPrefix + strings.Join(values, ", ") + ";\n"
	if _, err := writer.WriteString(sql); err != nil {
		return fmt.Errorf("写入数据失败: %w", err)
	}
	
	return nil
}

// openDBWithRetry 打开数据库连接，带重试和超时机制
func (m *MySQLBackup) openDBWithRetry(ctx context.Context, dsn string) (*sql.DB, error) {
	const maxRetries = 3
	const retryDelay = 2 * time.Second
	const connectionTimeout = 30 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			m.logger.Info("重试连接数据库", "attempt", i+1, "max_retries", maxRetries)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryDelay):
			}
		}

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			m.logger.Error("打开数据库连接失败", "error", err, "attempt", i+1)
			if i == maxRetries-1 {
				return nil, err
			}
			continue
		}

		// 设置连接参数
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		// 创建带超时的上下文
		pingCtx, cancel := context.WithTimeout(ctx, connectionTimeout)
		defer cancel()

		// 测试连接
		if err := db.PingContext(pingCtx); err != nil {
			m.logger.Error("数据库连接测试失败", "error", err, "attempt", i+1)
			db.Close()
			if i == maxRetries-1 {
				return nil, fmt.Errorf("数据库连接测试失败: %w", err)
			}
			continue
		}

		m.logger.Info("数据库连接成功", "attempt", i+1)
		return db, nil
	}

	return nil, fmt.Errorf("达到最大重试次数 %d", maxRetries)
}

// ValidateBackup 验证备份完整性
func (m *MySQLBackup) ValidateBackup(ctx context.Context, dsn string, backupFile string, tables []string) ([]backup.ValidationResult, error) {
	db, err := m.openDBWithRetry(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	defer db.Close()

	var results []backup.ValidationResult

	// 获取备份文件中的表统计信息
	backupCounts, err := m.getBackupTableCounts(backupFile)
	if err != nil {
		return nil, fmt.Errorf("分析备份文件失败: %w", err)
	}

	// 如果没有指定表，使用备份文件中的所有表
	if len(tables) == 0 {
		for table := range backupCounts {
			tables = append(tables, table)
		}
	}

	// 验证每个表
	for _, table := range tables {
		result := backup.ValidationResult{
			TableName: table,
			IsValid:   false,
		}

		// 获取源数据库中的记录数
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)
		if err := db.QueryRowContext(ctx, countQuery).Scan(&result.SourceCount); err != nil {
			result.ErrorMessage = fmt.Sprintf("获取源表记录数失败: %v", err)
			results = append(results, result)
			continue
		}

		// 获取备份文件中的记录数
		if count, exists := backupCounts[table]; exists {
			result.BackupCount = count
		} else {
			result.ErrorMessage = "备份文件中未找到该表"
			results = append(results, result)
			continue
		}

		// 比较记录数
		if result.SourceCount == result.BackupCount {
			result.IsValid = true
		} else {
			result.ErrorMessage = fmt.Sprintf("记录数不匹配: 源=%d, 备份=%d", result.SourceCount, result.BackupCount)
		}

		results = append(results, result)
	}

	return results, nil
}

// getBackupTableCounts 分析备份文件，统计每个表的记录数
func (m *MySQLBackup) getBackupTableCounts(backupFile string) (map[string]int64, error) {
	file, err := os.Open(backupFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	counts := make(map[string]int64)
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "INSERT INTO") {
			// 从 INSERT 语句中提取表名
			if tableName := m.extractTableNameFromInsert(line); tableName != "" {
				counts[tableName]++
			}
		}
	}

	return counts, scanner.Err()
}

// extractTableNameFromInsert 从 INSERT 语句中提取表名
func (m *MySQLBackup) extractTableNameFromInsert(insertSQL string) string {
	// INSERT INTO `table_name` 或 INSERT INTO table_name
	re := regexp.MustCompile(`INSERT\s+INTO\s+(?:\x60([^\x60]+)\x60|([^\s(]+))`)
	matches := re.FindStringSubmatch(insertSQL)
	if len(matches) > 2 {
		if matches[1] != "" {
			return matches[1] // 带反引号的表名
		}
		return matches[2] // 不带反引号的表名
	}
	return ""
}
