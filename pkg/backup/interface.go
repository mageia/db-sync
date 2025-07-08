package backup

import "context"

// ProgressCallback 进度回调函数类型
type ProgressCallback func(tableName string, processed, total int64)

// BackupOptions 定义备份选项
type BackupOptions struct {
	BatchSize        int              // 分批大小
	Tables           []string         // 指定表名，为空则备份所有表
	Logger           Logger           // 日志接口
	ProgressCallback ProgressCallback // 进度回调函数
}

// LoadOptions 定义恢复选项
type LoadOptions struct {
	BatchSize        int              // 分批大小
	Tables           []string         // 指定表名，为空则恢复所有表
	ClearBeforeLoad  bool             // 是否在加载前清空
	Logger           Logger           // 日志接口
	ProgressCallback ProgressCallback // 进度回调函数
}

// ValidationResult 验证结果
type ValidationResult struct {
	TableName    string
	SourceCount  int64
	BackupCount  int64
	IsValid      bool
	ErrorMessage string
}

// DatabaseBackup 定义数据库备份恢复接口
type DatabaseBackup interface {
	// Sync 备份数据库到文件
	Sync(ctx context.Context, dsn string, outputPath string, opts BackupOptions) error

	// Load 从备份文件恢复到数据库
	Load(ctx context.Context, backupFile string, dsn string, opts LoadOptions) error

	// ValidateBackup 验证备份完整性
	ValidateBackup(ctx context.Context, dsn string, backupFile string, tables []string) ([]ValidationResult, error)
}

// Logger 定义日志接口
type Logger interface {
	Info(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Debug(msg string, fields ...interface{})
}
