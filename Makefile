# 设置基本变量
BINARY_NAME=db-sync
MCP_BINARY_NAME=db-sync-mcp
VERSION=0.1.3
BUILD_DIR=build
MAIN_PATH=cmd/db-sync/main.go
MCP_MAIN_PATH=cmd/mcp-server/main.go

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
	@echo "  build-mcp   - Build MCP server binary for current platform"
	@echo "  build-all   - Build binaries for all platforms"
	@echo "  clean       - Remove build directory"
	@echo "  test        - Run tests"
	@echo "  lint        - Run golangci-lint"
	@echo "  fmt         - Format code"
	@echo "  package     - Build deb and rpm packages"
	@echo "  release     - Build release artifacts"
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

# 构建 MCP Server 二进制文件
.PHONY: build-mcp
build-mcp: init
	@echo "Building $(MCP_BINARY_NAME) version $(VERSION)..."
	@$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(MCP_BINARY_NAME) $(MCP_MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(MCP_BINARY_NAME)"

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

# 运行 lint 检查
.PHONY: lint
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found, installing..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	@golangci-lint run

# 格式化代码
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	@gofmt -s -w .
	@goimports -w .

# 构建包
.PHONY: package
package: build-all
	@echo "Building packages..."
	@mkdir -p $(BUILD_DIR)/packages
	@# 构建 Linux AMD64 DEB 包
	@mkdir -p $(BUILD_DIR)/deb-amd64/{DEBIAN,usr/bin}
	@cp $(BUILD_DIR)/$(BINARY_NAME)_linux_amd64 $(BUILD_DIR)/deb-amd64/usr/bin/$(BINARY_NAME)
	@echo "Package: $(BINARY_NAME)" > $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Version: $(VERSION)" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Section: utils" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Priority: optional" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Architecture: amd64" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Maintainer: auto-tunnel <noreply@github.com>" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo "Description: High-performance database backup and restore tool" >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@echo " DB-Sync is a command-line tool for backing up and restoring databases." >> $(BUILD_DIR)/deb-amd64/DEBIAN/control
	@dpkg-deb --build $(BUILD_DIR)/deb-amd64 $(BUILD_DIR)/packages/$(BINARY_NAME)_$(VERSION)_amd64.deb 2>/dev/null || echo "DEB package creation requires dpkg-deb"
	@echo "Package created: $(BUILD_DIR)/packages/$(BINARY_NAME)_$(VERSION)_amd64.deb"

# 构建发布包
.PHONY: release
release: package
	@echo "Creating release artifacts..."
	@mkdir -p $(BUILD_DIR)/release
	@cp $(BUILD_DIR)/$(BINARY_NAME)_* $(BUILD_DIR)/release/ 2>/dev/null || true
	@cp $(BUILD_DIR)/packages/* $(BUILD_DIR)/release/ 2>/dev/null || true
	@cd $(BUILD_DIR)/release && sha256sum * > checksums.txt
	@echo "Release artifacts created in $(BUILD_DIR)/release/"

# 开发环境设置
.PHONY: dev-setup
dev-setup:
	@echo "Setting up development environment..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@go mod download
	@echo "Development environment setup complete"

# 检查依赖
.PHONY: deps
deps:
	@echo "Checking dependencies..."
	@go mod verify
	@go mod tidy

# 安装二进制文件到本地
.PHONY: install
install: build
	@echo "Installing $(BINARY_NAME) to $(HOME)/bin..."
	@mkdir -p $(HOME)/bin
	@cp $(BUILD_DIR)/$(BINARY_NAME) $(HOME)/bin/
	@echo "Installation complete. Make sure $(HOME)/bin is in your PATH"
