package backup

import (
	"context"
	"time"
)

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

// SyncMode 同步模式
type SyncMode string

const (
	SyncModeFull        SyncMode = "full"        // 全量同步
	SyncModeIncremental SyncMode = "incremental" // 增量同步
)

// ConflictStrategy 冲突处理策略
type ConflictStrategy string

const (
	ConflictStrategySkip      ConflictStrategy = "skip"      // 跳过冲突记录
	ConflictStrategyOverwrite ConflictStrategy = "overwrite" // 覆盖目标记录
	ConflictStrategyFail      ConflictStrategy = "fail"      // 遇到冲突失败
)

// SyncOptions 同步选项
type SyncOptions struct {
	Mode             SyncMode                // 同步模式
	Tables           []string                // 指定表名，为空则同步所有表
	BatchSize        int                     // 分批大小
	ConflictStrategy ConflictStrategy        // 冲突处理策略
	TimestampColumn  string                  // 增量同步时间戳列名（增量模式必需）
	LastSyncTime     *time.Time              // 上次同步时间（增量模式使用）
	DryRun           bool                    // 是否为试运行
	Logger           Logger                  // 日志接口
	ProgressCallback ProgressCallback        // 进度回调函数
}

// SyncResult 同步结果
type SyncResult struct {
	TableName       string
	ProcessedRows   int64
	InsertedRows    int64
	UpdatedRows     int64
	SkippedRows     int64
	ErrorRows       int64
	Duration        time.Duration
	LastSyncTime    *time.Time
	ErrorMessage    string
}

// DatabaseBackup 定义数据库备份恢复接口
type DatabaseBackup interface {
	// Sync 备份数据库到文件
	Sync(ctx context.Context, dsn string, outputPath string, opts BackupOptions) error

	// Load 从备份文件恢复到数据库
	Load(ctx context.Context, backupFile string, dsn string, opts LoadOptions) error

	// ValidateBackup 验证备份完整性
	ValidateBackup(ctx context.Context, dsn string, backupFile string, tables []string) ([]ValidationResult, error)

	// SyncDatabase 数据库间同步
	SyncDatabase(ctx context.Context, sourceDSN, targetDSN string, opts SyncOptions) ([]SyncResult, error)
}

// Logger 定义日志接口
type Logger interface {
	Info(msg string, fields ...interface{})
	Error(msg string, fields ...interface{})
	Debug(msg string, fields ...interface{})
}
