#!/bin/bash

# DB-Sync Release Script
# This script helps create a new release with proper versioning and changelog

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 函数：打印彩色信息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查是否在 Git 仓库中
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    print_error "Not in a Git repository"
    exit 1
fi

# 检查是否有未提交的更改
if ! git diff-index --quiet HEAD --; then
    print_error "There are uncommitted changes. Please commit or stash them first."
    exit 1
fi

# 检查是否在 main 分支
current_branch=$(git branch --show-current)
if [ "$current_branch" != "main" ]; then
    print_warning "Not on main branch. Current branch: $current_branch"
    read -p "Do you want to continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 获取版本号
if [ -z "$1" ]; then
    print_error "Usage: $0 <version>"
    print_info "Example: $0 1.0.0"
    exit 1
fi

VERSION="$1"

# 验证版本格式 (semantic versioning)
if ! echo "$VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    print_error "Invalid version format. Use semantic versioning (e.g., 1.0.0)"
    exit 1
fi

# 检查标签是否已存在
if git tag -l | grep -q "^v$VERSION$"; then
    print_error "Tag v$VERSION already exists"
    exit 1
fi

print_info "Creating release v$VERSION"

# 更新 Makefile 中的版本号
print_info "Updating version in Makefile..."
sed -i.bak "s/VERSION=.*/VERSION=$VERSION/" Makefile
rm Makefile.bak

# 更新 CHANGELOG.md
print_info "Updating CHANGELOG.md..."
today=$(date +%Y-%m-%d)
sed -i.bak "s/## \[Unreleased\]/## [Unreleased]\n\n## [$VERSION] - $today/" CHANGELOG.md
rm CHANGELOG.md.bak

# 提交更改
print_info "Committing version bump..."
git add Makefile CHANGELOG.md
git commit -m "chore: bump version to $VERSION"

# 创建标签
print_info "Creating git tag v$VERSION..."
git tag -a "v$VERSION" -m "Release v$VERSION"

# 推送到远程
print_info "Pushing to origin..."
git push origin main
git push origin "v$VERSION"

print_success "Release v$VERSION created successfully!"
print_info "GitHub Actions will automatically build and publish the release."
print_info "Check the Actions tab on GitHub for build status."

# 显示后续步骤
echo
print_info "Next steps:"
echo "1. Monitor the GitHub Actions build at: https://github.com/auto-tunnel/db-sync/actions"
echo "2. Once the build completes, the release will be available at: https://github.com/auto-tunnel/db-sync/releases"
echo "3. Update any documentation or announcement posts as needed"