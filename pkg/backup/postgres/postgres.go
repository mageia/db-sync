package postgres

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
						vals[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
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

// SyncDatabase 数据库间同步
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

	var results []backup.SyncResult

	// 逐表同步
	for _, table := range tables {
		opts.Logger.Info("开始同步表", "table", table, "mode", opts.Mode)
		
		result, err := p.syncTable(ctx, sourceDB, targetDB, table, opts)
		if err != nil {
			opts.Logger.Error("同步表失败", "table", table, "error", err)
			result.ErrorMessage = err.Error()
		}
		
		results = append(results, result)
		
		// 如果遇到错误且策略为 fail，立即停止
		if err != nil && opts.ConflictStrategy == backup.ConflictStrategyFail {
			return results, fmt.Errorf("同步表 %s 失败: %w", table, err)
		}
	}

	return results, nil
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
			// 清空目标表（全量同步）
			truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", pq.QuoteIdentifier(table))
			if _, err := targetDB.ExecContext(ctx, truncateQuery); err != nil {
				return fmt.Errorf("清空目标表失败: %w", err)
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

	// 构建值占位符
	valuePlaceholders := make([]string, len(data))
	args := make([]interface{}, 0, len(data)*len(columns))
	
	for i, row := range data {
		placeholders := make([]string, len(columns))
		for j := range columns {
			placeholders[j] = fmt.Sprintf("$%d", len(args)+1)
			args = append(args, row[j])
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

	// 构建值占位符
	valuePlaceholders := make([]string, len(data))
	args := make([]interface{}, 0, len(data)*len(columns))
	
	for i, row := range data {
		placeholders := make([]string, len(columns))
		for j := range columns {
			placeholders[j] = fmt.Sprintf("$%d", len(args)+1)
			args = append(args, row[j])
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

// 其他辅助方法...
