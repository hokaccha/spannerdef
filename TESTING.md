# Testing Guide

## ローカルでのテスト実行

### 必要な環境

- Docker / Docker Compose
- gcloud CLI (for Spanner emulator setup)
- Go 1.21+

### 簡単な実行方法

```bash
# 統合テストを実行（emulatorの起動・停止も自動）
make test-local

# または直接スクリプトを実行
./test-local.sh
```

### 詳細なコマンド

```bash
# Unit テストのみ実行
make test-unit

# Integration テスト実行（emulator自動起動・停止）
make test-local

# Integration テスト実行（emulatorを起動したままにする）
make test-local-keep

# 起動中のemulatorを停止
make test-stop

# ヘルプ表示
make help
```

### 手動でのemulator操作

```bash
# Emulatorを起動
docker-compose up -d spanner-emulator

# 状態確認
docker-compose ps

# ログ確認
docker-compose logs spanner-emulator

# 停止
docker-compose down
```

## GitHub Actions

プッシュ・PR作成時に自動でテストが実行されます。

- Unit tests: `go test ./...`
- Integration tests: 21個の包括的テストケース

## テスト内容

### Unit Tests
- DDL parsing and generation logic
- Utility functions
- Schema comparison

### Integration Tests (21 test cases)
- **Basic DDL Operations** (6 tests)
  - Table creation and modification
  - Column addition/removal
  - Index creation/removal
- **Spanner-specific Features** (4 tests)
  - Array columns
  - Commit timestamp options
  - Index STORING clauses  
  - Interleaved tables
- **Edge Cases** (4 tests)
  - Empty schema handling
  - Drop protection (--enable-drop flag)
  - Complex data types
  - No-change scenarios
- **Advanced Scenarios** (7 tests)
  - Multiple tables and indexes
  - Export functionality
  - Configuration filtering

## トラブルシューティング

### Emulatorが起動しない

```bash
# ポートの使用状況確認
lsof -i :9010
lsof -i :9020

# Dockerの状況確認
docker-compose ps
docker-compose logs spanner-emulator
```

### gcloudエラー

```bash
# gcloud認証状態確認
gcloud auth list

# 設定確認
gcloud config list
```

### テストが失敗する

```bash
# 詳細ログでテスト実行
go test -v ./integration_test.go

# 環境変数の確認
env | grep SPANNER
```