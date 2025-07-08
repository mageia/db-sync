# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions workflow for automated building and releasing
- Support for building deb and rpm packages
- Comprehensive CI/CD pipeline with testing and linting
- golangci-lint configuration for code quality
- Enhanced Makefile with package building support

### Changed
- Improved README with better structure and badges
- Updated documentation with MySQL examples
- Enhanced installation instructions

## [1.0.0] - 2025-01-08

### Added
- Initial release of DB-Sync
- Support for PostgreSQL database backup and restore
- Support for MySQL database backup and restore
- Batch processing for large datasets
- Transaction-based operations for data consistency
- Flexible backup and restore options
- Table-level and database-level operations
- Graceful shutdown support with signal handling
- Comprehensive command-line interface
- Detailed logging functionality

### Features
- Cross-platform support (Linux, macOS, Windows)
- Multiple architectures (amd64, arm64)
- Customizable batch sizes
- Selective table backup/restore
- Clear option for table truncation during restore
- Connection string validation
- Error handling and recovery

### Dependencies
- Go 1.23+
- github.com/lib/pq for PostgreSQL support
- github.com/go-sql-driver/mysql for MySQL support
- github.com/spf13/pflag for command-line parsing