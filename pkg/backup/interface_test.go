package backup

import (
	"context"
	"testing"
)

// MockBackupRestore 实现 BackupRestore 接口用于测试
type MockBackupRestore struct {
	backupCalled  bool
	restoreCalled bool
}

func (m *MockBackupRestore) Backup(ctx context.Context, file string, tables []string, batchSize int) error {
	m.backupCalled = true
	return nil
}

func (m *MockBackupRestore) Restore(ctx context.Context, file string, tables []string, clear bool, batchSize int) error {
	m.restoreCalled = true
	return nil
}

func TestBackupRestore(t *testing.T) {
	mock := &MockBackupRestore{}
	
	// 测试 Backup 方法
	err := mock.Backup(context.Background(), "test.sql", []string{"table1"}, 1000)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if !mock.backupCalled {
		t.Error("Expected Backup to be called")
	}
	
	// 测试 Restore 方法
	err = mock.Restore(context.Background(), "test.sql", []string{"table1"}, false, 1000)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	
	if !mock.restoreCalled {
		t.Error("Expected Restore to be called")
	}
}