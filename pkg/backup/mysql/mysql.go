package mysql

import (
	"context"
	"database/sql"
	"db-sync/pkg/backup"
	"fmt"
	"os"
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
	db, err := sql.Open("mysql", dsn)
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

	// 创建输出文件
	file, err := os.Create(outputPath)
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

		if err := m.backupTable(ctx, tx, table, file, opts.BatchSize); err != nil {
			return fmt.Errorf("备份表 %s 失败: %w", table, err)
		}
	}

	// 写入文件尾
	if _, err := file.WriteString("\nSET FOREIGN_KEY_CHECKS=1;\n"); err != nil {
		return fmt.Errorf("写入文件尾失败: %w", err)
	}

	return tx.Commit()
}

func (m *MySQLBackup) backupTable(ctx context.Context, tx *sql.Tx, table string, file *os.File, batchSize int) error {
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

	// 分批查询数据
	offset := 0
	for {
		query := fmt.Sprintf("SELECT * FROM `%s` LIMIT %d OFFSET %d", table, batchSize, offset)
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
			insertSQL := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (", table, strings.Join(columns, "`, `"))
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
	db, err := sql.Open("mysql", dsn)
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
