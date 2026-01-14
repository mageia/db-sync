package postgres

import (
	"bufio"
	"context"
	"database/sql"
	"db-sync/pkg/backup"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type PostgresBackup struct {
	logger backup.Logger
}

// NewPostgresBackup 创建一个新的 PostgresBackup 实例
func NewPostgresBackup(logger backup.Logger) *PostgresBackup {
	return &PostgresBackup{
		logger: logger,
	}
}

// 在 PostgresBackup 结构体定义后添加这个私有方法
func ensureSSLMode(dsn string) string {
	if !strings.Contains(dsn, "sslmode=") {
		if strings.Contains(dsn, "?") {
			return dsn + "&sslmode=disable"
		}
		return dsn + "?sslmode=disable"
	}
	return dsn
}

func (p *PostgresBackup) Sync(ctx context.Context, dsn string, outputPath string, opts backup.BackupOptions) error {
	dsn = ensureSSLMode(dsn)
	db, err := p.openDBWithRetry(ctx, dsn)
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
		tables, err = p.getAllTables(ctx, tx)
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

	// 备份序列
	if err := p.backupSequences(ctx, tx, tables, file); err != nil {
		return fmt.Errorf("备份序列失败: %w", err)
	}

	// 逐表备份
	for _, table := range tables {
		p.logger.Info("开始备份表", "table", table)

		if err := p.backupTable(ctx, tx, table, file, opts.BatchSize, opts.ProgressCallback); err != nil {
			return fmt.Errorf("备份表 %s 失败: %w", table, err)
		}
	}

	return tx.Commit()
}

func (p *PostgresBackup) backupTable(ctx context.Context, tx *sql.Tx, table string, file *os.File, batchSize int, progressCallback backup.ProgressCallback) error {
	// 获取表结构
	schema, err := p.getTableSchema(ctx, tx, table)
	if err != nil {
		return err
	}

	// 写入表结构
	if _, err := fmt.Fprintf(file, "-- Table: %s\n%s\n", table, schema); err != nil {
		return err
	}

	// 获取列名
	columns, err := p.getTableColumns(ctx, tx, table)
	if err != nil {
		return fmt.Errorf("获取表列信息失败: %w", err)
	}

	// 获取表总记录数用于进度显示
	var totalRows int64
	if progressCallback != nil {
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
		if err := tx.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
			p.logger.Debug("获取表记录数失败", "table", table, "error", err)
			totalRows = 0 // 如果获取失败，设为0
		}
	}

	// 分批查询数据 - 使用参数化查询
	offset := 0
	processedRows := int64(0)
	for {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT $1 OFFSET $2", pq.QuoteIdentifier(table))
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
			quotedColumns[i] = pq.QuoteIdentifier(col)
		}
		insertPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", pq.QuoteIdentifier(table), strings.Join(quotedColumns, ", "))

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
						// 使用 PostgreSQL 的十六进制格式处理二进制数据 (bytea)
						vals[i] = fmt.Sprintf("'\\x%s'", hex.EncodeToString(v))
					case string:
						vals[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
					case time.Time:
						vals[i] = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999-07"))
					case bool:
						vals[i] = fmt.Sprintf("%t", v)
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
				if err := p.writeBatch(bufWriter, insertPrefix, batchValues); err != nil {
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
			if err := p.writeBatch(bufWriter, insertPrefix, batchValues); err != nil {
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
		p.logger.Debug("正在备份数据", "table", table, "offset", offset)
	}

	return nil
}

// getAllTables 获取数据库中所有表名
func (p *PostgresBackup) getAllTables(ctx context.Context, tx *sql.Tx) ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
	`

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

// getTableSchema 获取表结构
func (p *PostgresBackup) getTableSchema(ctx context.Context, tx *sql.Tx, table string) (string, error) {
	query := `
		SELECT 
			'CREATE TABLE ' || quote_ident(table_name) || ' (' ||
			string_agg(
				quote_ident(column_name) || ' ' ||
				data_type ||
				CASE 
					WHEN character_maximum_length IS NOT NULL 
					THEN '(' || character_maximum_length || ')'
					ELSE ''
				END ||
				CASE 
					WHEN is_nullable = 'NO' 
					THEN ' NOT NULL'
					ELSE ''
				END,
				', '
			) || ');'
		FROM information_schema.columns
		WHERE table_schema = 'public'
		AND table_name = $1
		GROUP BY table_name;
	`

	var createSQL string
	err := tx.QueryRowContext(ctx, query, table).Scan(&createSQL)
	if err != nil {
		return "", fmt.Errorf("获取表结构失败: %w", err)
	}

	return createSQL, nil
}

// getTableColumns 获取表的列名
func (p *PostgresBackup) getTableColumns(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	query := `
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1 
		ORDER BY ordinal_position
	`

	rows, err := tx.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return columns, nil
}

// tableExists 检查表是否存在
func (p *PostgresBackup) tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`
	
	var exists bool
	err := db.QueryRowContext(ctx, query, table).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("检查表是否存在失败: %w", err)
	}
	
	return exists, nil
}

// getTableSchemaFromDB 从数据库连接获取表结构（用于同步时的表创建）
func (p *PostgresBackup) getTableSchemaFromDB(ctx context.Context, db *sql.DB, table string) (string, error) {
	query := `
		SELECT 
			'CREATE TABLE ' || quote_ident(table_name) || ' (' ||
			string_agg(
				quote_ident(column_name) || ' ' ||
				data_type ||
				CASE 
					WHEN character_maximum_length IS NOT NULL 
					THEN '(' || character_maximum_length || ')'
					ELSE ''
				END ||
				CASE 
					WHEN is_nullable = 'NO' 
					THEN ' NOT NULL'
					ELSE ''
				END,
				', '
			) || ');'
		FROM information_schema.columns
		WHERE table_schema = 'public'
		AND table_name = $1
		GROUP BY table_name;
	`

	var createSQL string
	err := db.QueryRowContext(ctx, query, table).Scan(&createSQL)
	if err != nil {
		return "", fmt.Errorf("获取表结构失败: %w", err)
	}

	return createSQL, nil
}

// Load 从备份文件恢复到数据库
func (p *PostgresBackup) Load(ctx context.Context, backupFile string, dsn string, opts backup.LoadOptions) error {
	dsn = ensureSSLMode(dsn)
	db, err := p.openDBWithRetry(ctx, dsn)
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

		// 如果需要清空表
		if opts.ClearBeforeLoad && isCreateTableStatement(stmt) {
			dropStmt := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", tableName)
			if _, err := tx.ExecContext(ctx, dropStmt); err != nil {
				return fmt.Errorf("清空表失败: %w", err)
			}
		}

		// 执行SQL语句
		p.logger.Info("执行SQL语句", "table", tableName)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("执行SQL语句失败: %w", err)
		}
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
			return strings.Trim(parts[2], `"`)
		}
	}
	return ""
}

// 辅助函数：判断是否是CREATE TABLE语句
func isCreateTableStatement(stmt string) bool {
	stmt = strings.TrimSpace(strings.ToUpper(stmt))
	return strings.HasPrefix(stmt, "CREATE TABLE")
}

// 新增：备份序列
func (p *PostgresBackup) backupSequences(ctx context.Context, tx *sql.Tx, tables []string, file *os.File) error {
	// 使用 pg_sequences 视图，使用正确的列名
	query := `
		SELECT
			sequencename as sequence_name,
			start_value,
			increment_by,
			max_value,
			min_value,
			cache_size,
			last_value,
			cycle
		FROM pg_sequences
		WHERE schemaname = 'public'
		AND sequencename LIKE '%_seq'
	`

	if len(tables) > 0 {
		// 如果指定了表，只备份这些表相关的序列
		query = `
			SELECT
				s.sequencename as sequence_name,
				s.start_value,
				s.increment_by,
				s.max_value,
				s.min_value,
				s.cache_size,
				s.last_value,
				s.cycle
			FROM pg_sequences s
			JOIN pg_tables t ON 
				t.tablename = ANY($1) AND
				s.sequencename LIKE t.tablename || '_%_seq'
			WHERE s.schemaname = 'public'
		`
	}

	rows, err := tx.QueryContext(ctx, query, pq.Array(tables))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			seqName                                            string
			startVal, incBy, maxVal, minVal, cacheVal, lastVal sql.NullInt64
			cycle                                              bool // 改为 bool 类型
		)

		if err := rows.Scan(&seqName, &startVal, &incBy, &maxVal, &minVal, &cacheVal, &lastVal, &cycle); err != nil {
			return err
		}

		// 写入序列重建语句
		if _, err := fmt.Fprintf(file, "\n-- Sequence: %s\n", seqName); err != nil {
			return err
		}

		// 删除序列（如果存在）
		if _, err := fmt.Fprintf(file, "DROP SEQUENCE IF EXISTS %s CASCADE;\n", seqName); err != nil {
			return err
		}

		// 创建序列
		createSQL := fmt.Sprintf("CREATE SEQUENCE %s\n", seqName)
		if incBy.Valid {
			createSQL += fmt.Sprintf("    INCREMENT BY %d\n", incBy.Int64)
		}
		if minVal.Valid {
			createSQL += fmt.Sprintf("    MINVALUE %d\n", minVal.Int64)
		}
		if maxVal.Valid {
			createSQL += fmt.Sprintf("    MAXVALUE %d\n", maxVal.Int64)
		}
		if startVal.Valid {
			createSQL += fmt.Sprintf("    START WITH %d\n", startVal.Int64)
		}
		if cacheVal.Valid {
			createSQL += fmt.Sprintf("    CACHE %d\n", cacheVal.Int64)
		}
		if cycle { // 直接使用 bool 值
			createSQL += "    CYCLE"
		}
		createSQL += ";\n"

		if _, err := fmt.Fprint(file, createSQL); err != nil {
			return err
		}

		// 设置序列当前值
		if lastVal.Valid {
			if _, err := fmt.Fprintf(file, "SELECT setval('%s', %d, true);\n", seqName, lastVal.Int64); err != nil {
				return err
			}
		}
	}

	return rows.Err()
}

// writeBatch 批量写入 INSERT 语句
func (p *PostgresBackup) writeBatch(writer *bufio.Writer, insertPrefix string, values []string) error {
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
func (p *PostgresBackup) openDBWithRetry(ctx context.Context, dsn string) (*sql.DB, error) {
	const maxRetries = 3
	const retryDelay = 2 * time.Second
	const connectionTimeout = 30 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			p.logger.Info("重试连接数据库", "attempt", i+1, "max_retries", maxRetries)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryDelay):
			}
		}

		db, err := sql.Open("postgres", dsn)
		if err != nil {
			p.logger.Error("打开数据库连接失败", "error", err, "attempt", i+1)
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
			p.logger.Error("数据库连接测试失败", "error", err, "attempt", i+1)
			db.Close()
			if i == maxRetries-1 {
				return nil, fmt.Errorf("数据库连接测试失败: %w", err)
			}
			continue
		}

		p.logger.Info("数据库连接成功", "attempt", i+1)
		return db, nil
	}

	return nil, fmt.Errorf("达到最大重试次数 %d", maxRetries)
}

// ValidateBackup 验证备份完整性
func (p *PostgresBackup) ValidateBackup(ctx context.Context, dsn string, backupFile string, tables []string) ([]backup.ValidationResult, error) {
	dsn = ensureSSLMode(dsn)
	db, err := p.openDBWithRetry(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	defer db.Close()

	var results []backup.ValidationResult

	// 获取备份文件中的表统计信息
	backupCounts, err := p.getBackupTableCounts(backupFile)
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
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
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
func (p *PostgresBackup) getBackupTableCounts(backupFile string) (map[string]int64, error) {
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
			if tableName := extractTableNameFromInsert(line); tableName != "" {
				counts[tableName]++
			}
		}
	}

	return counts, scanner.Err()
}

// extractTableNameFromInsert 从 INSERT 语句中提取表名
func extractTableNameFromInsert(insertSQL string) string {
	// INSERT INTO "table_name" 或 INSERT INTO table_name
	re := regexp.MustCompile(`INSERT\s+INTO\s+(?:"([^"]+)"|([^\s(]+))`)
	matches := re.FindStringSubmatch(insertSQL)
	if len(matches) > 2 {
		if matches[1] != "" {
			return matches[1] // 带引号的表名
		}
		return matches[2] // 不带引号的表名
	}
	return ""
}

// tableRetryInfo 记录表的重试信息
type tableRetryInfo struct {
	tableName    string
	retryCount   int
	lastError    error
	dependencies []string // 依赖的表
}

// syncWithRetryQueue 使用重试队列处理表同步（处理外键约束）
func (p *PostgresBackup) syncWithRetryQueue(ctx context.Context, sourceDB, targetDB *sql.DB, tables []string, opts backup.SyncOptions) ([]backup.SyncResult, error) {
	const maxRetries = 3 // 最大重试次数
	
	var results []backup.SyncResult
	retryQueue := make([]*tableRetryInfo, 0)
	completedTables := make(map[string]bool)
	
	// 初始化重试队列
	for _, table := range tables {
		retryQueue = append(retryQueue, &tableRetryInfo{
			tableName:  table,
			retryCount: 0,
		})
	}
	
	// 处理队列直到为空或所有表都达到最大重试次数
	for len(retryQueue) > 0 {
		currentQueue := retryQueue
		retryQueue = make([]*tableRetryInfo, 0)
		hasProgress := false // 标记本轮是否有进展
		
		for _, tableInfo := range currentQueue {
			// 检查是否已完成
			if completedTables[tableInfo.tableName] {
				continue
			}
			
			opts.Logger.Info("同步表", "table", tableInfo.tableName, "retry", tableInfo.retryCount, "mode", opts.Mode)
			
			// 尝试同步表
			result, err := p.syncTable(ctx, sourceDB, targetDB, tableInfo.tableName, opts)
			
			if err != nil {
				// 检查是否是外键约束错误
				if p.isForeignKeyError(err) && tableInfo.retryCount < maxRetries {
					tableInfo.retryCount++
					tableInfo.lastError = err
					
					// 尝试解析依赖关系
					deps := p.extractDependencies(err)
					if len(deps) > 0 {
						tableInfo.dependencies = deps
					}
					
					// 重新加入队列
					retryQueue = append(retryQueue, tableInfo)
					opts.Logger.Info("表同步因外键约束失败，加入重试队列", 
						"table", tableInfo.tableName, 
						"retry_count", tableInfo.retryCount,
						"max_retries", maxRetries,
						"dependencies", strings.Join(tableInfo.dependencies, ","))
				} else {
					// 达到最大重试次数或非外键错误
					result.ErrorMessage = err.Error()
					if tableInfo.retryCount >= maxRetries {
						opts.Logger.Error("表同步达到最大重试次数", 
							"table", tableInfo.tableName, 
							"retries", tableInfo.retryCount, 
							"error", err)
						result.ErrorMessage = fmt.Sprintf("达到最大重试次数(%d): %v", maxRetries, err)
					} else {
						opts.Logger.Error("表同步失败", "table", tableInfo.tableName, "error", err)
					}
					results = append(results, result)
					
					// 如果策略为 fail，立即停止
					if opts.ConflictStrategy == backup.ConflictStrategyFail {
						// 将剩余的表标记为跳过
						for _, remaining := range retryQueue {
							skipResult := backup.SyncResult{
								TableName:    remaining.tableName,
								ErrorMessage: "因前置表失败而跳过",
							}
							results = append(results, skipResult)
						}
						return results, fmt.Errorf("同步表 %s 失败: %w", tableInfo.tableName, err)
					}
				}
			} else {
				// 同步成功
				completedTables[tableInfo.tableName] = true
				results = append(results, result)
				hasProgress = true
				opts.Logger.Info("表同步成功", "table", tableInfo.tableName, "rows", result.ProcessedRows)
			}
		}
		
		// 如果本轮没有任何进展，检查是否有循环依赖
		if !hasProgress && len(retryQueue) > 0 {
			// 尝试禁用外键检查同步剩余的表
			opts.Logger.Info("检测到可能的循环依赖，尝试禁用外键检查同步剩余表", "count", len(retryQueue))
			
			// PostgreSQL禁用外键约束的方式
			tx, err := targetDB.BeginTx(ctx, nil)
			if err != nil {
				opts.Logger.Error("开启事务失败", "error", err)
			} else {
				// 在事务中禁用触发器（包括外键约束）
				if _, err := tx.ExecContext(ctx, "SET session_replication_role = 'replica'"); err != nil {
					opts.Logger.Error("禁用外键检查失败", "error", err)
					tx.Rollback()
				} else {
					// 重试剩余的表
					for _, tableInfo := range retryQueue {
						result, err := p.syncTableInTx(ctx, sourceDB, tx, tableInfo.tableName, opts)
						if err != nil {
							result.ErrorMessage = fmt.Sprintf("禁用外键后仍失败: %v", err)
							opts.Logger.Error("禁用外键检查后同步仍失败", "table", tableInfo.tableName, "error", err)
						} else {
							completedTables[tableInfo.tableName] = true
							opts.Logger.Info("禁用外键检查后同步成功", "table", tableInfo.tableName)
						}
						results = append(results, result)
					}
					
					// 恢复外键检查
					if _, err := tx.ExecContext(ctx, "SET session_replication_role = 'origin'"); err != nil {
						opts.Logger.Error("恢复外键检查失败", "error", err)
					}
					
					// 提交事务
					if err := tx.Commit(); err != nil {
						opts.Logger.Error("提交事务失败", "error", err)
					}
					
					// 清空重试队列
					retryQueue = nil
				}
			}
		}
	}
	
	return results, nil
}

// syncTableInTx 在事务中同步表（用于禁用外键约束的场景）
func (p *PostgresBackup) syncTableInTx(ctx context.Context, sourceDB *sql.DB, tx *sql.Tx, table string, opts backup.SyncOptions) (backup.SyncResult, error) {
	startTime := time.Now()
	result := backup.SyncResult{
		TableName: table,
	}

	// 获取表结构信息
	columns, primaryKeys, err := p.getTableInfo(ctx, sourceDB, table)
	if err != nil {
		return result, fmt.Errorf("获取表信息失败: %w", err)
	}

	// 根据同步模式执行不同的同步策略（使用事务）
	switch opts.Mode {
	case backup.SyncModeFull:
		err = p.syncTableFullInTx(ctx, sourceDB, tx, table, columns, primaryKeys, opts, &result)
	case backup.SyncModeIncremental:
		// 增量模式暂不支持事务内执行
		return result, fmt.Errorf("增量模式不支持事务内执行")
	default:
		return result, fmt.Errorf("不支持的同步模式: %s", opts.Mode)
	}

	result.Duration = time.Since(startTime)
	return result, err
}

// syncTableFullInTx 在事务中执行全量同步
func (p *PostgresBackup) syncTableFullInTx(ctx context.Context, sourceDB *sql.DB, tx *sql.Tx, table string, columns, primaryKeys []string, opts backup.SyncOptions, result *backup.SyncResult) error {
	// 清空目标表
	truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", pq.QuoteIdentifier(table))
	if _, err := tx.ExecContext(ctx, truncateQuery); err != nil {
		return fmt.Errorf("清空目标表失败: %w", err)
	}

	// 获取源表总记录数
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
	if err := sourceDB.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf("获取源表记录数失败: %w", err)
	}

	// 分批读取源数据并插入目标数据库
	offset := 0
	for {
		// 查询源数据
		orderByClause := ""
		if len(primaryKeys) > 0 {
			orderByClause = fmt.Sprintf(" ORDER BY %s", strings.Join(primaryKeys, ", "))
		}
		selectQuery := fmt.Sprintf("SELECT * FROM %s%s LIMIT $1 OFFSET $2",
			pq.QuoteIdentifier(table), orderByClause)
		
		rows, err := sourceDB.QueryContext(ctx, selectQuery, opts.BatchSize, offset)
		if err != nil {
			return fmt.Errorf("查询源数据失败: %w", err)
		}

		count := 0
		var insertData [][]interface{}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("扫描源数据失败: %w", err)
			}

			insertData = append(insertData, values)
			count++
		}
		rows.Close()

		// 批量插入目标数据库
		if len(insertData) > 0 {
			if err := p.batchInsertInTx(ctx, tx, table, columns, insertData); err != nil {
				return fmt.Errorf("批量插入失败: %w", err)
			}
		}

		result.ProcessedRows += int64(count)
		result.InsertedRows += int64(count)

		// 更新进度
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(table, result.ProcessedRows, totalRows)
		}

		if count < opts.BatchSize {
			break
		}
		offset += opts.BatchSize
	}

	return nil
}

// batchInsertInTx 在事务中批量插入数据
func (p *PostgresBackup) batchInsertInTx(ctx context.Context, tx *sql.Tx, table string, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// 构建插入语句
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = pq.QuoteIdentifier(col)
	}

	// 构建值占位符，对 []byte 类型使用 decode($n, 'hex') 格式
	valuePlaceholders := make([]string, len(data))
	args := make([]interface{}, 0, len(data)*len(columns))
	
	for i, row := range data {
		placeholders := make([]string, len(columns))
		for j := range columns {
			paramIdx := len(args) + 1
			// 检查是否是 []byte 类型，如果是则使用 decode 函数
			if b, ok := row[j].([]byte); ok && row[j] != nil {
				placeholders[j] = fmt.Sprintf("decode($%d, 'hex')", paramIdx)
				args = append(args, hex.EncodeToString(b))
			} else {
				placeholders[j] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, row[j])
			}
		}
		valuePlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		pq.QuoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(valuePlaceholders, ", "))

	_, err := tx.ExecContext(ctx, insertQuery, args...)
	return err
}

// isForeignKeyError 检查错误是否是外键约束错误
func (p *PostgresBackup) isForeignKeyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "foreign key") || 
		   strings.Contains(errStr, "violates foreign key constraint") ||
		   strings.Contains(errStr, "23503") || // PostgreSQL错误码：foreign_key_violation
		   strings.Contains(errStr, "23502") || // PostgreSQL错误码：not_null_violation (可能由外键引起)
		   strings.Contains(errStr, "referential integrity") ||
		   strings.Contains(errStr, "constraint")
}

// extractDependencies 从错误信息中提取依赖的表名
func (p *PostgresBackup) extractDependencies(err error) []string {
	if err == nil {
		return nil
	}
	
	var dependencies []string
	errStr := err.Error()
	
	// PostgreSQL 外键错误通常包含 "table" 和 referenced table 信息
	// 例如: violates foreign key constraint "fk_orders_customers" on table "orders"
	// DETAIL: Key (customer_id)=(1) is not present in table "customers".
	re := regexp.MustCompile(`in table "([^"]+)"`)
	matches := re.FindAllStringSubmatch(errStr, -1)
	for _, match := range matches {
		if len(match) > 1 {
			dependencies = append(dependencies, match[1])
		}
	}
	
	// 也尝试从 constraint 名称中提取表名
	re2 := regexp.MustCompile(`constraint "([^"]+)"`)
	matches2 := re2.FindAllStringSubmatch(errStr, -1)
	for _, match := range matches2 {
		if len(match) > 1 {
			// 通常约束名格式为 fk_table1_table2
			parts := strings.Split(match[1], "_")
			if len(parts) > 2 {
				// 取最后一个部分作为可能的表名
				dependencies = append(dependencies, parts[len(parts)-1])
			}
		}
	}
	
	return dependencies
}

// SyncDatabase 数据库间同步（支持外键约束重试）
func (p *PostgresBackup) SyncDatabase(ctx context.Context, sourceDSN, targetDSN string, opts backup.SyncOptions) ([]backup.SyncResult, error) {
	sourceDSN = ensureSSLMode(sourceDSN)
	targetDSN = ensureSSLMode(targetDSN)

	// 连接源数据库
	sourceDB, err := p.openDBWithRetry(ctx, sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("连接源数据库失败: %w", err)
	}
	defer sourceDB.Close()

	// 连接目标数据库
	targetDB, err := p.openDBWithRetry(ctx, targetDSN)
	if err != nil {
		return nil, fmt.Errorf("连接目标数据库失败: %w", err)
	}
	defer targetDB.Close()

	// 获取要同步的表列表
	tables := opts.Tables
	if len(tables) == 0 {
		tables, err = p.getAllTablesFromDB(ctx, sourceDB)
		if err != nil {
			return nil, fmt.Errorf("获取源数据库表列表失败: %w", err)
		}
	}

	// 使用重试队列机制处理外键约束
	results, err := p.syncWithRetryQueue(ctx, sourceDB, targetDB, tables, opts)
	if err != nil {
		return results, err
	}

	// 同步序列 (id_seq)
	if err := p.syncSequences(ctx, sourceDB, targetDB, tables, opts); err != nil {
		opts.Logger.Error("同步序列失败", "error", err)
		// 为序列同步创建结果记录
		seqResult := backup.SyncResult{
			TableName:    "sequences",
			ErrorMessage: fmt.Sprintf("同步序列失败: %v", err),
		}
		results = append(results, seqResult)
		
		if opts.ConflictStrategy == backup.ConflictStrategyFail {
			return results, fmt.Errorf("同步序列失败: %w", err)
		}
	} else {
		opts.Logger.Info("序列同步完成")
	}

	return results, nil
}

// ValidateData 验证两个PostgreSQL数据库之间的数据一致性
func (p *PostgresBackup) ValidateData(ctx context.Context, sourceDSN, targetDSN string, opts backup.DataValidationOptions) ([]backup.DataValidationResult, error) {
	sourceDSN = ensureSSLMode(sourceDSN)
	targetDSN = ensureSSLMode(targetDSN)

	// 连接源数据库
	sourceDB, err := p.openDBWithRetry(ctx, sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("连接源数据库失败: %w", err)
	}
	defer sourceDB.Close()

	// 连接目标数据库
	targetDB, err := p.openDBWithRetry(ctx, targetDSN)
	if err != nil {
		return nil, fmt.Errorf("连接目标数据库失败: %w", err)
	}
	defer targetDB.Close()

	// 获取要校验的表列表
	tables := opts.Tables
	if len(tables) == 0 {
		tables, err = p.getAllTablesFromDB(ctx, sourceDB)
		if err != nil {
			return nil, fmt.Errorf("获取源数据库表列表失败: %w", err)
		}
	}

	var results []backup.DataValidationResult
	
	// 逐表校验
	for _, table := range tables {
		opts.Logger.Info("开始校验表", "table", table)
		
		result := backup.DataValidationResult{
			TableName: table,
			IsValid:   false,
		}

		// 获取表结构信息
		columns, primaryKeys, err := p.getTableInfo(ctx, sourceDB, table)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("获取源表信息失败: %v", err)
			results = append(results, result)
			continue
		}

		// 检查目标表是否存在
		exists, err := p.tableExists(ctx, targetDB, table)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("检查目标表失败: %v", err)
			results = append(results, result)
			continue
		}
		if !exists {
			result.ErrorMessage = "目标表不存在"
			results = append(results, result)
			continue
		}

		// 获取行数
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
		if err := sourceDB.QueryRowContext(ctx, countQuery).Scan(&result.SourceRows); err != nil {
			result.ErrorMessage = fmt.Sprintf("获取源表行数失败: %v", err)
			results = append(results, result)
			continue
		}
		if err := targetDB.QueryRowContext(ctx, countQuery).Scan(&result.TargetRows); err != nil {
			result.ErrorMessage = fmt.Sprintf("获取目标表行数失败: %v", err)
			results = append(results, result)
			continue
		}

		// 如果行数不一致，直接标记为不一致
		if result.SourceRows != result.TargetRows {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("行数不一致: 源表=%d, 目标表=%d", result.SourceRows, result.TargetRows)
			results = append(results, result)
			continue
		}

		// 如果不需要比较实际数据，只要行数一致就认为校验通过
		if !opts.CompareData {
			result.IsValid = true
			results = append(results, result)
			continue
		}

		// 计算校验和 - 使用当前表的列，而不是 opts.ChecksumColumns（避免跨表复用）
		checksumColumns := columns
		if len(opts.ChecksumColumns) > 0 {
			checksumColumns = opts.ChecksumColumns
		}
		
		sourceChecksum, err := p.calculateTableChecksum(ctx, sourceDB, table, checksumColumns, primaryKeys)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("计算源表校验和失败: %v", err)
			results = append(results, result)
			continue
		}
		result.SourceChecksum = sourceChecksum

		targetChecksum, err := p.calculateTableChecksum(ctx, targetDB, table, checksumColumns, primaryKeys)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("计算目标表校验和失败: %v", err)
			results = append(results, result)
			continue
		}
		result.TargetChecksum = targetChecksum

		// 比较校验和
		if sourceChecksum == targetChecksum {
			result.IsValid = true
		} else {
			result.IsValid = false
			result.ErrorMessage = "数据校验和不匹配"
			
			// 如果需要，进行抽样比较找出不匹配的数据
			if opts.SampleSize > 0 {
				mismatches, err := p.findDataMismatches(ctx, sourceDB, targetDB, table, columns, primaryKeys, int64(opts.SampleSize))
				if err != nil {
					opts.Logger.Error("查找不匹配数据失败", "table", table, "error", err)
				} else {
					result.Details = mismatches
					result.MismatchedRows = int64(len(mismatches))
					result.SampleSize = int64(opts.SampleSize)
					if result.SampleSize > result.SourceRows {
						result.SampleSize = result.SourceRows
					}
					result.MatchedRows = result.SampleSize - result.MismatchedRows
				}
			}
		}

		results = append(results, result)
		
		// 更新进度
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(table, int64(len(results)), int64(len(tables)))
		}
	}

	return results, nil
}

// calculateTableChecksum 计算表的校验和
func (p *PostgresBackup) calculateTableChecksum(ctx context.Context, db *sql.DB, table string, columns, primaryKeys []string) (string, error) {
	// 构建校验和查询
	// 使用 MD5 和字符串连接计算整个表的校验和
	columnList := make([]string, len(columns))
	for i, col := range columns {
		// 使用 COALESCE 处理 NULL 值，并转换为文本
		columnList[i] = fmt.Sprintf("COALESCE(%s::text, 'NULL')", pq.QuoteIdentifier(col))
	}
	
	// 构建排序子句，primaryKeys 已经被 pq.QuoteIdentifier 包装过了
	orderByClause := strings.Join(primaryKeys, ", ")
	if len(primaryKeys) == 0 {
		orderByClause = strings.Join(columnList, ", ")
	}
	
	// PostgreSQL 使用 STRING_AGG 和 MD5 计算整个表的校验和
	query := fmt.Sprintf(`
		SELECT MD5(STRING_AGG(
			MD5(%s),
			''
			ORDER BY %s
		)) AS checksum
		FROM %s
	`, strings.Join(columnList, " || ',' || "), 
	   orderByClause, 
	   pq.QuoteIdentifier(table))
	
	var checksum sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&checksum); err != nil {
		return "", err
	}
	
	if !checksum.Valid {
		return "", nil
	}
	
	return checksum.String, nil
}

// findDataMismatches 查找数据不匹配的记录
func (p *PostgresBackup) findDataMismatches(ctx context.Context, sourceDB, targetDB *sql.DB, table string, columns, primaryKeys []string, sampleSize int64) ([]backup.DataMismatchDetail, error) {
	if len(primaryKeys) == 0 {
		// 没有主键，无法精确比较
		return nil, fmt.Errorf("表 %s 没有主键，无法进行数据比较", table)
	}

	var mismatches []backup.DataMismatchDetail
	
	// 构建查询，使用 LIMIT 进行抽样
	orderBy := strings.Join(primaryKeys, ", ")
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT $1", pq.QuoteIdentifier(table), orderBy)
	
	// 查询源数据
	sourceRows, err := sourceDB.QueryContext(ctx, query, sampleSize)
	if err != nil {
		return nil, fmt.Errorf("查询源数据失败: %w", err)
	}
	defer sourceRows.Close()
	
	// 处理每一行
	for sourceRows.Next() {
		// 扫描源数据
		sourceValues := make([]interface{}, len(columns))
		sourceValuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			sourceValuePtrs[i] = &sourceValues[i]
		}
		
		if err := sourceRows.Scan(sourceValuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描源数据失败: %w", err)
		}
		
		// 构建主键条件
		primaryKeyValues := make(map[string]interface{})
		whereConditions := make([]string, 0)
		whereArgs := make([]interface{}, 0)
		argIndex := 1
		
		for i, col := range columns {
			// 检查是否是主键
			isPK := false
			for _, pk := range primaryKeys {
				pkName := strings.Trim(pk, `"`)
				if strings.Contains(pk, ".") {
					// 处理 schema.column 格式
					parts := strings.Split(pk, ".")
					pkName = strings.Trim(parts[len(parts)-1], `"`)
				}
				if pkName == col {
					isPK = true
					break
				}
			}
			if isPK {
				primaryKeyValues[col] = sourceValues[i]
				whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", pq.QuoteIdentifier(col), argIndex))
				whereArgs = append(whereArgs, sourceValues[i])
				argIndex++
			}
		}
		
		// 查询目标数据库中对应的记录
		targetQuery := fmt.Sprintf("SELECT * FROM %s WHERE %s", pq.QuoteIdentifier(table), strings.Join(whereConditions, " AND "))
		targetRow := targetDB.QueryRowContext(ctx, targetQuery, whereArgs...)
		
		targetValues := make([]interface{}, len(columns))
		targetValuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			targetValuePtrs[i] = &targetValues[i]
		}
		
		if err := targetRow.Scan(targetValuePtrs...); err != nil {
			if err == sql.ErrNoRows {
				// 目标表中没有对应记录
				mismatches = append(mismatches, backup.DataMismatchDetail{
					PrimaryKey:  primaryKeyValues,
					ColumnName:  "(记录不存在)",
					SourceValue: "(存在)",
					TargetValue: "(不存在)",
				})
			}
			continue
		}
		
		// 比较每个列的值
		for i, col := range columns {
			if !p.compareValues(sourceValues[i], targetValues[i]) {
				mismatches = append(mismatches, backup.DataMismatchDetail{
					PrimaryKey:  primaryKeyValues,
					ColumnName:  col,
					SourceValue: sourceValues[i],
					TargetValue: targetValues[i],
				})
			}
		}
	}
	
	return mismatches, nil
}

// compareValues 比较两个值是否相等
func (p *PostgresBackup) compareValues(a, b interface{}) bool {
	// 处理 NULL 值
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	
	// 将 []byte 转换为 string 进行比较
	aBytes, aIsByte := a.([]byte)
	bBytes, bIsByte := b.([]byte)
	
	if aIsByte && bIsByte {
		return string(aBytes) == string(bBytes)
	}
	
	if aIsByte {
		return string(aBytes) == fmt.Sprintf("%v", b)
	}
	
	if bIsByte {
		return fmt.Sprintf("%v", a) == string(bBytes)
	}
	
	// 处理时间类型
	aTime, aIsTime := a.(time.Time)
	bTime, bIsTime := b.(time.Time)
	
	if aIsTime && bIsTime {
		// 比较时间，忽略纳秒级别的差异
		return aTime.Unix() == bTime.Unix()
	}
	
	// 使用反射进行深度比较
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// syncTableSchema 同步表结构，确保目标表结构与源表一致
func (p *PostgresBackup) syncTableSchema(ctx context.Context, sourceDB, targetDB *sql.DB, table string, opts backup.SyncOptions) error {
	// 获取源表的列信息
	sourceColumns, err := p.getTableColumnsDetailed(ctx, sourceDB, table)
	if err != nil {
		return fmt.Errorf("获取源表列信息失败: %w", err)
	}
	
	// 获取目标表的列信息
	targetColumns, err := p.getTableColumnsDetailed(ctx, targetDB, table)
	if err != nil {
		return fmt.Errorf("获取目标表列信息失败: %w", err)
	}
	
	// 创建列映射
	targetColumnMap := make(map[string]map[string]string)
	for _, col := range targetColumns {
		targetColumnMap[col["column_name"]] = col
	}
	
	// 检查并添加缺失的列
	for _, sourceCol := range sourceColumns {
		colName := sourceCol["column_name"]
		
		if targetCol, exists := targetColumnMap[colName]; !exists {
			// 目标表缺少该列，需要添加
			alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
				pq.QuoteIdentifier(table),
				pq.QuoteIdentifier(colName),
				sourceCol["column_type"])
			
			// 添加默认值
			if sourceCol["column_default"] != "" && sourceCol["column_default"] != "NULL" {
				alterSQL += " DEFAULT " + sourceCol["column_default"]
			}
			
			// 添加 NOT NULL 约束
			if sourceCol["is_nullable"] == "NO" {
				// 先添加列允许NULL，然后更新默认值，最后设置NOT NULL
				tempSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
					pq.QuoteIdentifier(table),
					pq.QuoteIdentifier(colName),
					sourceCol["column_type"])
				
				if _, err := targetDB.ExecContext(ctx, tempSQL); err != nil {
					opts.Logger.Error("添加列失败", "table", table, "column", colName, "error", err)
					continue
				}
				
				// 如果有默认值，先更新为默认值
				if sourceCol["column_default"] != "" && sourceCol["column_default"] != "NULL" {
					updateSQL := fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL",
						pq.QuoteIdentifier(table),
						pq.QuoteIdentifier(colName),
						sourceCol["column_default"],
						pq.QuoteIdentifier(colName))
					targetDB.ExecContext(ctx, updateSQL)
				}
				
				// 设置 NOT NULL
				notNullSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
					pq.QuoteIdentifier(table),
					pq.QuoteIdentifier(colName))
				targetDB.ExecContext(ctx, notNullSQL)
			} else {
				if _, err := targetDB.ExecContext(ctx, alterSQL); err != nil {
					opts.Logger.Error("添加列失败", "table", table, "column", colName, "error", err)
					continue
				}
			}
			
			opts.Logger.Info("已添加缺失的列", "table", table, "column", colName, "type", sourceCol["column_type"])
		} else {
			// 检查列类型是否一致
			if sourceCol["column_type"] != targetCol["column_type"] {
				// 尝试修改列类型
				alterSQL := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
					pq.QuoteIdentifier(table),
					pq.QuoteIdentifier(colName),
					sourceCol["column_type"],
					pq.QuoteIdentifier(colName),
					sourceCol["column_type"])
				
				if _, err := targetDB.ExecContext(ctx, alterSQL); err != nil {
					opts.Logger.Error("修改列类型失败", "table", table, "column", colName, 
						"source_type", sourceCol["column_type"], 
						"target_type", targetCol["column_type"], 
						"error", err)
				} else {
					opts.Logger.Info("已修改列类型", "table", table, "column", colName,
						"old_type", targetCol["column_type"],
						"new_type", sourceCol["column_type"])
				}
			}
		}
	}
	
	// 检查是否有多余的列（目标表有但源表没有的列）
	sourceColumnMap := make(map[string]bool)
	for _, col := range sourceColumns {
		sourceColumnMap[col["column_name"]] = true
	}
	
	for colName := range targetColumnMap {
		if !sourceColumnMap[colName] {
			opts.Logger.Info("目标表存在额外的列", "table", table, "column", colName)
			// 可以选择删除，但这里我们保留它以避免数据丢失
		}
	}
	
	return nil
}

// getTableConstraints 获取表的约束信息
func (p *PostgresBackup) getTableConstraints(ctx context.Context, db *sql.DB, table string) ([]map[string]string, error) {
	query := `
		SELECT 
			c.conname AS constraint_name,
			c.contype AS constraint_type,
			pg_get_constraintdef(c.oid) AS constraint_definition
		FROM pg_constraint c
		JOIN pg_class t ON c.conrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE t.relname = $1
		AND n.nspname = 'public'
		AND c.contype IN ('u', 'p', 'f', 'c')  -- unique, primary, foreign, check
		ORDER BY c.contype, c.conname
	`
	
	rows, err := db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var constraints []map[string]string
	for rows.Next() {
		var name, ctype, definition string
		if err := rows.Scan(&name, &ctype, &definition); err != nil {
			return nil, err
		}
		
		constraints = append(constraints, map[string]string{
			"name":       name,
			"type":       ctype,
			"definition": definition,
		})
	}
	
	return constraints, nil
}

// getTableIndexes 获取表的索引信息
func (p *PostgresBackup) getTableIndexes(ctx context.Context, db *sql.DB, table string) ([]map[string]string, error) {
	query := `
		SELECT 
			i.relname AS index_name,
			pg_get_indexdef(i.oid) AS index_definition,
			CASE WHEN idx.indisunique THEN 'true' ELSE 'false' END AS is_unique,
			CASE WHEN idx.indisprimary THEN 'true' ELSE 'false' END AS is_primary
		FROM pg_index idx
		JOIN pg_class t ON idx.indrelid = t.oid
		JOIN pg_class i ON idx.indexrelid = i.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE t.relname = $1
		AND n.nspname = 'public'
		AND NOT idx.indisprimary  -- 排除主键索引（已在约束中处理）
		ORDER BY i.relname
	`
	
	rows, err := db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var indexes []map[string]string
	for rows.Next() {
		var name, definition, isUnique, isPrimary string
		if err := rows.Scan(&name, &definition, &isUnique, &isPrimary); err != nil {
			return nil, err
		}
		
		indexes = append(indexes, map[string]string{
			"name":       name,
			"definition": definition,
			"is_unique":  isUnique,
			"is_primary": isPrimary,
		})
	}
	
	return indexes, nil
}

// dropTableConstraints 删除表的约束（除了主键）
func (p *PostgresBackup) dropTableConstraints(ctx context.Context, db *sql.DB, table string, opts backup.SyncOptions) error {
	// 获取所有约束
	constraints, err := p.getTableConstraints(ctx, db, table)
	if err != nil {
		return fmt.Errorf("获取约束信息失败: %w", err)
	}
	
	// 删除外键和唯一约束（保留主键）
	for _, constraint := range constraints {
		ctype := constraint["type"]
		name := constraint["name"]
		
		// 跳过主键约束
		if ctype == "p" {
			continue
		}
		
		// 删除约束
		dropSQL := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s CASCADE",
			pq.QuoteIdentifier(table),
			pq.QuoteIdentifier(name))
		
		if _, err := db.ExecContext(ctx, dropSQL); err != nil {
			opts.Logger.Error("删除约束失败", "table", table, "constraint", name, "error", err)
		} else {
			opts.Logger.Debug("已删除约束", "table", table, "constraint", name, "type", ctype)
		}
	}
	
	// 删除非主键索引
	indexes, err := p.getTableIndexes(ctx, db, table)
	if err != nil {
		return fmt.Errorf("获取索引信息失败: %w", err)
	}
	
	for _, index := range indexes {
		name := index["name"]
		
		dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s CASCADE", pq.QuoteIdentifier(name))
		
		if _, err := db.ExecContext(ctx, dropSQL); err != nil {
			opts.Logger.Error("删除索引失败", "table", table, "index", name, "error", err)
		} else {
			opts.Logger.Debug("已删除索引", "table", table, "index", name)
		}
	}
	
	return nil
}

// recreateTableConstraints 重建表的约束和索引
func (p *PostgresBackup) recreateTableConstraints(ctx context.Context, sourceDB, targetDB *sql.DB, table string, opts backup.SyncOptions) error {
	// 获取源表的约束
	constraints, err := p.getTableConstraints(ctx, sourceDB, table)
	if err != nil {
		return fmt.Errorf("获取源表约束信息失败: %w", err)
	}
	
	// 重建约束（除了主键，主键应该已经存在）
	for _, constraint := range constraints {
		ctype := constraint["type"]
		name := constraint["name"]
		definition := constraint["definition"]
		
		// 跳过主键
		if ctype == "p" {
			continue
		}
		
		// 创建约束
		addSQL := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
			pq.QuoteIdentifier(table),
			pq.QuoteIdentifier(name),
			definition)
		
		if _, err := targetDB.ExecContext(ctx, addSQL); err != nil {
			// 如果约束已存在，忽略错误
			if !strings.Contains(err.Error(), "already exists") {
				opts.Logger.Error("创建约束失败", "table", table, "constraint", name, "error", err)
			}
		} else {
			opts.Logger.Debug("已创建约束", "table", table, "constraint", name)
		}
	}
	
	// 获取源表的索引
	indexes, err := p.getTableIndexes(ctx, sourceDB, table)
	if err != nil {
		return fmt.Errorf("获取源表索引信息失败: %w", err)
	}
	
	// 重建索引
	for _, index := range indexes {
		definition := index["definition"]
		
		if _, err := targetDB.ExecContext(ctx, definition); err != nil {
			// 如果索引已存在，忽略错误
			if !strings.Contains(err.Error(), "already exists") {
				opts.Logger.Error("创建索引失败", "table", table, "index", index["name"], "error", err)
			}
		} else {
			opts.Logger.Debug("已创建索引", "table", table, "index", index["name"])
		}
	}
	
	return nil
}

// getTableColumnsDetailed 获取表的详细列信息
func (p *PostgresBackup) getTableColumnsDetailed(ctx context.Context, db *sql.DB, table string) ([]map[string]string, error) {
	query := `
		SELECT 
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default,
			udt_name
		FROM information_schema.columns
		WHERE table_name = $1
		AND table_schema = 'public'
		ORDER BY ordinal_position
	`
	
	rows, err := db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var columns []map[string]string
	for rows.Next() {
		var columnName, dataType, isNullable, udtName string
		var charMaxLength, numericPrecision, numericScale sql.NullInt64
		var columnDefault sql.NullString
		
		if err := rows.Scan(&columnName, &dataType, &charMaxLength, &numericPrecision, 
			&numericScale, &isNullable, &columnDefault, &udtName); err != nil {
			return nil, err
		}
		
		// 构建完整的列类型
		columnType := dataType
		if dataType == "character varying" && charMaxLength.Valid {
			columnType = fmt.Sprintf("varchar(%d)", charMaxLength.Int64)
		} else if dataType == "character" && charMaxLength.Valid {
			columnType = fmt.Sprintf("char(%d)", charMaxLength.Int64)
		} else if dataType == "numeric" && numericPrecision.Valid {
			if numericScale.Valid && numericScale.Int64 > 0 {
				columnType = fmt.Sprintf("numeric(%d,%d)", numericPrecision.Int64, numericScale.Int64)
			} else {
				columnType = fmt.Sprintf("numeric(%d)", numericPrecision.Int64)
			}
		} else if dataType == "USER-DEFINED" {
			// 使用 udt_name 作为类型（如自定义枚举类型）
			columnType = udtName
		}
		
		col := map[string]string{
			"column_name":    columnName,
			"column_type":    columnType,
			"is_nullable":    isNullable,
			"column_default": "",
		}
		
		if columnDefault.Valid {
			col["column_default"] = columnDefault.String
		}
		
		columns = append(columns, col)
	}
	
	return columns, nil
}

// syncTable 同步单个表
func (p *PostgresBackup) syncTable(ctx context.Context, sourceDB, targetDB *sql.DB, table string, opts backup.SyncOptions) (backup.SyncResult, error) {
	startTime := time.Now()
	result := backup.SyncResult{
		TableName: table,
	}

	// 获取表结构信息
	columns, primaryKeys, err := p.getTableInfo(ctx, sourceDB, table)
	if err != nil {
		return result, fmt.Errorf("获取表信息失败: %w", err)
	}

	// 根据同步模式执行不同的同步策略
	switch opts.Mode {
	case backup.SyncModeFull:
		err = p.syncTableFull(ctx, sourceDB, targetDB, table, columns, primaryKeys, opts, &result)
	case backup.SyncModeIncremental:
		err = p.syncTableIncremental(ctx, sourceDB, targetDB, table, columns, primaryKeys, opts, &result)
	default:
		return result, fmt.Errorf("不支持的同步模式: %s", opts.Mode)
	}

	result.Duration = time.Since(startTime)
	return result, err
}

// syncTableFull 全量同步表
func (p *PostgresBackup) syncTableFull(ctx context.Context, sourceDB, targetDB *sql.DB, table string, columns, primaryKeys []string, opts backup.SyncOptions, result *backup.SyncResult) error {
	// 获取源表总记录数
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))
	if err := sourceDB.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf("获取源表记录数失败: %w", err)
	}

	if !opts.DryRun {
		// 检查目标表是否存在，如果不存在则创建
		exists, err := p.tableExists(ctx, targetDB, table)
		if err != nil {
			return fmt.Errorf("检查目标表是否存在失败: %w", err)
		}
		
		if !exists {
			// 获取源表结构
			createSQL, err := p.getTableSchemaFromDB(ctx, sourceDB, table)
			if err != nil {
				return fmt.Errorf("获取源表结构失败: %w", err)
			}
			
			// 在目标数据库创建表
			if _, err := targetDB.ExecContext(ctx, createSQL); err != nil {
				return fmt.Errorf("创建目标表失败: %w", err)
			}
			
			opts.Logger.Info("已创建目标表", "table", table)
		} else {
			// 如果启用了自动修复模式，检查并同步表结构
			if opts.AutoFixSchema {
				opts.Logger.Info("检查表结构差异", "table", table)
				if err := p.syncTableSchema(ctx, sourceDB, targetDB, table, opts); err != nil {
					opts.Logger.Error("同步表结构失败", "table", table, "error", err)
					// 不中断同步，尝试继续
				}
			}
			
			// 删除目标表的约束和索引（保留主键）
			opts.Logger.Info("临时删除约束和索引", "table", table)
			if err := p.dropTableConstraints(ctx, targetDB, table, opts); err != nil {
				opts.Logger.Error("删除约束失败", "table", table, "error", err)
			}
			
			// 清空目标表（全量同步）
			// 先尝试 TRUNCATE
			truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", pq.QuoteIdentifier(table))
			if _, err := targetDB.ExecContext(ctx, truncateQuery); err != nil {
				// 如果 TRUNCATE 失败（可能由于外键约束），尝试 DELETE
				opts.Logger.Info("TRUNCATE 失败，尝试使用 DELETE", "table", table, "error", err)
				deleteQuery := fmt.Sprintf("DELETE FROM %s", pq.QuoteIdentifier(table))
				if _, err := targetDB.ExecContext(ctx, deleteQuery); err != nil {
					return fmt.Errorf("清空目标表失败: %w", err)
				}
				// 重置序列
				resetSeqQuery := fmt.Sprintf(`
					DO $$
					DECLARE
						seq_name text;
					BEGIN
						FOR seq_name IN 
							SELECT pg_get_serial_sequence('%s', column_name) 
							FROM information_schema.columns 
							WHERE table_name = '%s' 
							AND column_default LIKE 'nextval%%'
						LOOP
							IF seq_name IS NOT NULL THEN
								EXECUTE 'ALTER SEQUENCE ' || seq_name || ' RESTART WITH 1';
							END IF;
						END LOOP;
					END $$;
				`, table, table)
				targetDB.ExecContext(ctx, resetSeqQuery)
			}
		}
	}

	// 分批读取源数据并插入目标数据库
	offset := 0
	for {
		// 查询源数据
		selectQuery := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT $1 OFFSET $2", 
			pq.QuoteIdentifier(table), strings.Join(primaryKeys, ", "))
		
		rows, err := sourceDB.QueryContext(ctx, selectQuery, opts.BatchSize, offset)
		if err != nil {
			return fmt.Errorf("查询源数据失败: %w", err)
		}

		count := 0
		var insertData [][]interface{}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("扫描源数据失败: %w", err)
			}

			insertData = append(insertData, values)
			count++
		}
		rows.Close()

		// 批量插入目标数据库
		if len(insertData) > 0 && !opts.DryRun {
			if err := p.batchInsert(ctx, targetDB, table, columns, insertData); err != nil {
				return fmt.Errorf("批量插入失败: %w", err)
			}
		}

		result.ProcessedRows += int64(count)
		result.InsertedRows += int64(count)

		// 更新进度
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(table, result.ProcessedRows, totalRows)
		}

		if count < opts.BatchSize {
			break
		}
		offset += opts.BatchSize
	}

	// 数据插入完成后，重建约束和索引
	if !opts.DryRun {
		opts.Logger.Info("重建约束和索引", "table", table)
		if err := p.recreateTableConstraints(ctx, sourceDB, targetDB, table, opts); err != nil {
			opts.Logger.Error("重建约束失败", "table", table, "error", err)
			// 不中断，因为数据已经同步完成
		}
	}

	return nil
}

// getAllTablesFromDB 从数据库连接获取所有表名
func (p *PostgresBackup) getAllTablesFromDB(ctx context.Context, db *sql.DB) ([]string, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	return p.getAllTables(ctx, tx)
}

// getTableInfo 获取表的列信息和主键信息
func (p *PostgresBackup) getTableInfo(ctx context.Context, db *sql.DB, table string) (columns, primaryKeys []string, err error) {
	// 获取列信息
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	columns, err = p.getTableColumns(ctx, tx, table)
	if err != nil {
		return nil, nil, err
	}

	// 获取主键信息
	pkQuery := `
		SELECT a.attname
		FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
		JOIN pg_class t ON t.oid = c.conrelid
		WHERE c.contype = 'p' AND t.relname = $1 AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
		ORDER BY array_position(c.conkey, a.attnum)
	`
	
	rows, err := tx.QueryContext(ctx, pkQuery, table)
	if err != nil {
		return columns, nil, fmt.Errorf("获取主键信息失败: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pkColumn string
		if err := rows.Scan(&pkColumn); err != nil {
			return columns, nil, err
		}
		primaryKeys = append(primaryKeys, pq.QuoteIdentifier(pkColumn))
	}

	// 如果没有主键，使用所有列排序（不推荐，但作为备选）
	if len(primaryKeys) == 0 {
		for _, col := range columns {
			primaryKeys = append(primaryKeys, pq.QuoteIdentifier(col))
		}
	}

	return columns, primaryKeys, nil
}

// batchInsert 批量插入数据
func (p *PostgresBackup) batchInsert(ctx context.Context, db *sql.DB, table string, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// 构建插入语句
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = pq.QuoteIdentifier(col)
	}

	// 构建值占位符，对 []byte 类型使用 decode($n, 'hex') 格式
	valuePlaceholders := make([]string, len(data))
	args := make([]interface{}, 0, len(data)*len(columns))
	
	for i, row := range data {
		placeholders := make([]string, len(columns))
		for j := range columns {
			paramIdx := len(args) + 1
			// 检查是否是 []byte 类型，如果是则使用 decode 函数
			if b, ok := row[j].([]byte); ok && row[j] != nil {
				placeholders[j] = fmt.Sprintf("decode($%d, 'hex')", paramIdx)
				args = append(args, hex.EncodeToString(b))
			} else {
				placeholders[j] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, row[j])
			}
		}
		valuePlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		pq.QuoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(valuePlaceholders, ", "))

	_, err := db.ExecContext(ctx, insertQuery, args...)
	return err
}

// syncTableIncremental 增量同步表
func (p *PostgresBackup) syncTableIncremental(ctx context.Context, sourceDB, targetDB *sql.DB, table string, columns, primaryKeys []string, opts backup.SyncOptions, result *backup.SyncResult) error {
	if opts.TimestampColumn == "" {
		return fmt.Errorf("增量同步必须指定时间戳列名")
	}

	// 构建时间戳条件
	var timeCondition string
	var timeArgs []interface{}
	if opts.LastSyncTime != nil {
		timeCondition = fmt.Sprintf("WHERE %s > $1", pq.QuoteIdentifier(opts.TimestampColumn))
		timeArgs = append(timeArgs, *opts.LastSyncTime)
	} else {
		// 如果没有指定最后同步时间，默认同步最近24小时的数据
		timeCondition = fmt.Sprintf("WHERE %s > NOW() - INTERVAL '24 hours'", pq.QuoteIdentifier(opts.TimestampColumn))
	}

	// 获取需要同步的记录数
	var totalRows int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", pq.QuoteIdentifier(table), timeCondition)
	if err := sourceDB.QueryRowContext(ctx, countQuery, timeArgs...).Scan(&totalRows); err != nil {
		return fmt.Errorf("获取增量记录数失败: %w", err)
	}

	if totalRows == 0 {
		opts.Logger.Info("表无增量数据", "table", table)
		return nil
	}

	// 分批处理增量数据
	offset := 0
	var maxTimestamp *time.Time

	for {
		// 查询增量数据，按时间戳排序
		selectQuery := fmt.Sprintf("SELECT * FROM %s %s ORDER BY %s LIMIT $%d OFFSET $%d",
			pq.QuoteIdentifier(table), timeCondition, pq.QuoteIdentifier(opts.TimestampColumn),
			len(timeArgs)+1, len(timeArgs)+2)
		
		queryArgs := append(timeArgs, opts.BatchSize, offset)
		rows, err := sourceDB.QueryContext(ctx, selectQuery, queryArgs...)
		if err != nil {
			return fmt.Errorf("查询增量数据失败: %w", err)
		}

		count := 0
		var upsertData [][]interface{}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("扫描增量数据失败: %w", err)
			}

			// 记录最大时间戳
			for i, col := range columns {
				if col == opts.TimestampColumn {
					if ts, ok := values[i].(time.Time); ok {
						if maxTimestamp == nil || ts.After(*maxTimestamp) {
							maxTimestamp = &ts
						}
					}
				}
			}

			upsertData = append(upsertData, values)
			count++
		}
		rows.Close()

		// 执行 UPSERT 操作
		if len(upsertData) > 0 && !opts.DryRun {
			insertedRows, updatedRows, err := p.batchUpsert(ctx, targetDB, table, columns, primaryKeys, upsertData, opts.ConflictStrategy)
			if err != nil {
				return fmt.Errorf("批量 UPSERT 失败: %w", err)
			}
			result.InsertedRows += insertedRows
			result.UpdatedRows += updatedRows
		}

		result.ProcessedRows += int64(count)

		// 更新进度
		if opts.ProgressCallback != nil {
			opts.ProgressCallback(table, result.ProcessedRows, totalRows)
		}

		if count < opts.BatchSize {
			break
		}
		offset += opts.BatchSize
	}

	// 记录最后同步时间
	result.LastSyncTime = maxTimestamp

	return nil
}

// batchUpsert 批量 UPSERT 操作
func (p *PostgresBackup) batchUpsert(ctx context.Context, db *sql.DB, table string, columns, primaryKeys []string, data [][]interface{}, strategy backup.ConflictStrategy) (insertedRows, updatedRows int64, err error) {
	if len(data) == 0 {
		return 0, 0, nil
	}

	// 构建 UPSERT 语句
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = pq.QuoteIdentifier(col)
	}

	// 构建值占位符，对 []byte 类型使用 decode($n, 'hex') 格式
	valuePlaceholders := make([]string, len(data))
	args := make([]interface{}, 0, len(data)*len(columns))
	
	for i, row := range data {
		placeholders := make([]string, len(columns))
		for j := range columns {
			paramIdx := len(args) + 1
			// 检查是否是 []byte 类型，如果是则使用 decode 函数
			if b, ok := row[j].([]byte); ok && row[j] != nil {
				placeholders[j] = fmt.Sprintf("decode($%d, 'hex')", paramIdx)
				args = append(args, hex.EncodeToString(b))
			} else {
				placeholders[j] = fmt.Sprintf("$%d", paramIdx)
				args = append(args, row[j])
			}
		}
		valuePlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	// 构建 PRIMARY KEY 约束名（用于冲突检测）
	pkColumns := make([]string, len(primaryKeys))
	for i, pk := range primaryKeys {
		// 移除引号，因为 ON CONFLICT 不需要引号
		pkColumns[i] = strings.Trim(pk, `"`)
	}

	var upsertQuery string
	switch strategy {
	case backup.ConflictStrategySkip:
		// 跳过冲突记录
		upsertQuery = fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES %s 
			ON CONFLICT (%s) DO NOTHING`,
			pq.QuoteIdentifier(table),
			strings.Join(quotedColumns, ", "),
			strings.Join(valuePlaceholders, ", "),
			strings.Join(pkColumns, ", "))
			
	case backup.ConflictStrategyOverwrite:
		// 覆盖冲突记录
		updateSets := make([]string, 0, len(columns))
		for _, col := range quotedColumns {
			if !contains(pkColumns, strings.Trim(col, `"`)) {
				updateSets = append(updateSets, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
		}
		
		upsertQuery = fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES %s 
			ON CONFLICT (%s) DO UPDATE SET %s`,
			pq.QuoteIdentifier(table),
			strings.Join(quotedColumns, ", "),
			strings.Join(valuePlaceholders, ", "),
			strings.Join(pkColumns, ", "),
			strings.Join(updateSets, ", "))
			
	case backup.ConflictStrategyFail:
		// 遇到冲突就失败
		upsertQuery = fmt.Sprintf(`
			INSERT INTO %s (%s) VALUES %s`,
			pq.QuoteIdentifier(table),
			strings.Join(quotedColumns, ", "),
			strings.Join(valuePlaceholders, ", "))
	}

	result, err := db.ExecContext(ctx, upsertQuery, args...)
	if err != nil {
		return 0, 0, err
	}

	rowsAffected, _ := result.RowsAffected()
	
	// PostgreSQL 的 INSERT ... ON CONFLICT 统计方式：
	// - 新插入的记录算作 affected rows
	// - 更新的记录不算作 affected rows (对于 DO UPDATE)
	// - 跳过的记录不算作 affected rows (对于 DO NOTHING)
	
	switch strategy {
	case backup.ConflictStrategySkip, backup.ConflictStrategyFail:
		return rowsAffected, 0, nil
	case backup.ConflictStrategyOverwrite:
		// 估算插入和更新的数量（PostgreSQL 不提供精确分解）
		totalExpected := int64(len(data))
		if rowsAffected < totalExpected {
			return rowsAffected, totalExpected - rowsAffected, nil
		}
		return rowsAffected, 0, nil
	}

	return rowsAffected, 0, nil
}

// contains 检查字符串切片是否包含指定字符串
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// syncSequences 同步序列
func (p *PostgresBackup) syncSequences(ctx context.Context, sourceDB, targetDB *sql.DB, tables []string, opts backup.SyncOptions) error {
	// 获取源数据库中的序列信息
	sequences, err := p.getSequenceInfo(ctx, sourceDB, tables)
	if err != nil {
		return fmt.Errorf("获取源数据库序列信息失败: %w", err)
	}

	if len(sequences) == 0 {
		opts.Logger.Info("没有找到需要同步的序列")
		return nil
	}

	opts.Logger.Info("开始同步序列", "count", len(sequences))

	// 逐个同步序列
	for _, seq := range sequences {
		if opts.DryRun {
			opts.Logger.Info("试运行：将同步序列", "sequence", seq.Name, "last_value", seq.LastValue)
			continue
		}

		// 创建或更新序列
		if err := p.createOrUpdateSequence(ctx, targetDB, seq); err != nil {
			return fmt.Errorf("同步序列 %s 失败: %w", seq.Name, err)
		}

		opts.Logger.Info("序列同步完成", "sequence", seq.Name, "last_value", seq.LastValue)
	}

	return nil
}

// SequenceInfo 序列信息结构
type SequenceInfo struct {
	Name        string
	StartValue  sql.NullInt64
	IncrementBy sql.NullInt64
	MaxValue    sql.NullInt64
	MinValue    sql.NullInt64
	CacheSize   sql.NullInt64
	LastValue   sql.NullInt64
	Cycle       bool
}

// getSequenceInfo 获取序列信息
func (p *PostgresBackup) getSequenceInfo(ctx context.Context, db *sql.DB, tables []string) ([]SequenceInfo, error) {
	var query string
	var args []interface{}

	if len(tables) == 0 {
		// 获取所有序列
		query = `
			SELECT
				sequencename as sequence_name,
				start_value,
				increment_by,
				max_value,
				min_value,
				cache_size,
				last_value,
				cycle
			FROM pg_sequences
			WHERE schemaname = 'public'
			ORDER BY sequencename
		`
	} else {
		// 只获取指定表相关的序列
		query = `
			SELECT
				s.sequencename as sequence_name,
				s.start_value,
				s.increment_by,
				s.max_value,
				s.min_value,
				s.cache_size,
				s.last_value,
				s.cycle
			FROM pg_sequences s
			WHERE s.schemaname = 'public'
			AND (
				s.sequencename = ANY($1) OR
				EXISTS (
					SELECT 1 FROM unnest($1) as t(table_name)
					WHERE s.sequencename LIKE t.table_name || '_%_seq'
				)
			)
			ORDER BY s.sequencename
		`
		args = append(args, pq.Array(tables))
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("查询序列信息失败: %w", err)
	}
	defer rows.Close()

	var sequences []SequenceInfo
	for rows.Next() {
		var seq SequenceInfo
		if err := rows.Scan(
			&seq.Name,
			&seq.StartValue,
			&seq.IncrementBy,
			&seq.MaxValue,
			&seq.MinValue,
			&seq.CacheSize,
			&seq.LastValue,
			&seq.Cycle,
		); err != nil {
			return nil, fmt.Errorf("扫描序列信息失败: %w", err)
		}
		sequences = append(sequences, seq)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("序列信息查询出错: %w", err)
	}

	return sequences, nil
}

// createOrUpdateSequence 创建或更新序列
func (p *PostgresBackup) createOrUpdateSequence(ctx context.Context, db *sql.DB, seq SequenceInfo) error {
	// 检查序列是否存在
	var exists bool
	checkQuery := `
		SELECT EXISTS (
			SELECT 1 FROM pg_sequences
			WHERE schemaname = 'public' AND sequencename = $1
		)
	`
	if err := db.QueryRowContext(ctx, checkQuery, seq.Name).Scan(&exists); err != nil {
		return fmt.Errorf("检查序列是否存在失败: %w", err)
	}

	if !exists {
		// 创建新序列
		if err := p.createSequence(ctx, db, seq); err != nil {
			return fmt.Errorf("创建序列失败: %w", err)
		}
	}

	// 更新序列当前值
	if seq.LastValue.Valid {
		setValueQuery := fmt.Sprintf("SELECT setval('%s', $1, true)", seq.Name)
		if _, err := db.ExecContext(ctx, setValueQuery, seq.LastValue.Int64); err != nil {
			return fmt.Errorf("设置序列当前值失败: %w", err)
		}
	}

	return nil
}

// createSequence 创建序列
func (p *PostgresBackup) createSequence(ctx context.Context, db *sql.DB, seq SequenceInfo) error {
	var createSQL strings.Builder
	createSQL.WriteString(fmt.Sprintf("CREATE SEQUENCE %s", pq.QuoteIdentifier(seq.Name)))

	if seq.IncrementBy.Valid {
		createSQL.WriteString(fmt.Sprintf(" INCREMENT BY %d", seq.IncrementBy.Int64))
	}
	if seq.MinValue.Valid {
		createSQL.WriteString(fmt.Sprintf(" MINVALUE %d", seq.MinValue.Int64))
	}
	if seq.MaxValue.Valid {
		createSQL.WriteString(fmt.Sprintf(" MAXVALUE %d", seq.MaxValue.Int64))
	}
	if seq.StartValue.Valid {
		createSQL.WriteString(fmt.Sprintf(" START WITH %d", seq.StartValue.Int64))
	}
	if seq.CacheSize.Valid {
		createSQL.WriteString(fmt.Sprintf(" CACHE %d", seq.CacheSize.Int64))
	}
	if seq.Cycle {
		createSQL.WriteString(" CYCLE")
	}

	if _, err := db.ExecContext(ctx, createSQL.String()); err != nil {
		return fmt.Errorf("执行创建序列SQL失败: %w", err)
	}

	return nil
}

// 其他辅助方法...
