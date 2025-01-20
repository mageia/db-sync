# 设置基本变量
BINARY_NAME=db-sync
VERSION=1.0.0
BUILD_DIR=build
MAIN_PATH=cmd/db-sync/main.go

# 获取当前 git commit hash（静默处理错误）
COMMIT_HASH=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Go 相关设置
GO=go
GOFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.CommitHash=$(COMMIT_HASH)"

# 支持的平台列表
PLATFORMS=linux-amd64 linux-arm64 darwin-amd64 darwin-arm64 windows-amd64

# 显示帮助信息
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build binary for current platform"
	@echo "  build-all   - Build binaries for all platforms"
	@echo "  clean       - Remove build directory"
	@echo "  test        - Run tests"
	@echo "  help        - Show this help message" 

# 清理构建目录
.PHONY: clean
clean:
	@rm -rf $(BUILD_DIR)

# 创建构建目录
.PHONY: init
init:
	@mkdir -p $(BUILD_DIR)

# 构建当前平台的二进制文件
.PHONY: build
build: init
	@echo "Building $(BINARY_NAME) version $(VERSION)..."
	@$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# 构建所有平台的二进制文件
.PHONY: build-all
build-all: clean init $(PLATFORMS)

# 为每个平台构建二进制文件
.PHONY: $(PLATFORMS)
$(PLATFORMS):
	$(eval PLATFORM_SPLIT=$(subst -, ,$@))
	$(eval GOOS=$(word 1,$(PLATFORM_SPLIT)))
	$(eval GOARCH=$(word 2,$(PLATFORM_SPLIT)))
	$(eval BINARY_SUFFIX=$(if $(filter windows,$(GOOS)),.exe,))
	
	@echo "Building for $(GOOS)/$(GOARCH)..."
	@GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(GOFLAGS) \
		-o $(BUILD_DIR)/$(BINARY_NAME)_$(GOOS)_$(GOARCH)$(BINARY_SUFFIX) \
		$(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)_$(GOOS)_$(GOARCH)$(BINARY_SUFFIX)"

# 运行测试
.PHONY: test
test:
	@echo "Running tests..."
	@$(GO) test -v ./...