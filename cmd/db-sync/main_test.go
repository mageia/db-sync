package main

import (
	"testing"
)

func TestMain(t *testing.T) {
	// 基本的测试函数，确保 main 包可以正常编译
	t.Log("Main package test passed")
}

func TestCLILogger(t *testing.T) {
	logger := &CLILogger{}
	
	// 测试 Info 方法
	logger.Info("Test info message")
	logger.Info("Test info with fields", "key1", "value1", "key2", "value2")
	
	// 测试 Error 方法
	logger.Error("Test error message")
	logger.Error("Test error with fields", "error", "test error")
	
	t.Log("CLILogger test passed")
}