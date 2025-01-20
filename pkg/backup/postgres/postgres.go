package postgres

import (
	"context"
	"database/sql"
	"db-sync/pkg/backup"
	"fmt"
	"os"
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
	db, err := sql.Open("postgres", dsn)
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

	// 创建输出文件
	file, err := os.Create(outputPath)
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

		if err := p.backupTable(ctx, tx, table, file, opts.BatchSize); err != nil {
			return fmt.Errorf("备份表 %s 失败: %w", table, err)
		}
	}

	return tx.Commit()
}

func (p *PostgresBackup) backupTable(ctx context.Context, tx *sql.Tx, table string, file *os.File, batchSize int) error {
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

	// 分批查询数据
	offset := 0
	for {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, batchSize, offset)
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}

		count := 0
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

			// 构建 INSERT 语句
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (", table, strings.Join(columns, ", "))
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
			insertSQL += strings.Join(vals, ", ") + ");\n"

			// 写入 INSERT 语句
			if _, err := file.WriteString(insertSQL); err != nil {
				rows.Close()
				return fmt.Errorf("写入数据失败: %w", err)
			}

			count++
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

// Load 从备份文件恢复到数据库
func (p *PostgresBackup) Load(ctx context.Context, backupFile string, dsn string, opts backup.LoadOptions) error {
	dsn = ensureSSLMode(dsn)
	db, err := sql.Open("postgres", dsn)
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

// 其他辅助方法...
