# Testing Guide

## ローカルでのテスト実行

### 必要な環境

- Docker / Docker Compose  
- gcloud CLI (for Spanner emulator setup)
- Go 1.21+

### 安全性について

Integration testsは**Spanner emulatorでのみ実行**されます：

- 実際のSpannerインスタンスに接続することはありません
- `SPANNER_EMULATOR_HOST`が設定されていない場合、テストはスキップされます
- デフォルト値（test-project等）が使用されるため、設定ミスのリスクがありません

### 簡単な実行方法

```bash
# 統合テストを実行（emulatorの起動・停止も自動）
make test-local

# または直接スクリプトを実行
./test-local.sh
```

### 環境変数不要での実行

**NEW**: `SPANNER_EMULATOR_HOST`を指定せずにテストを実行できます：

```bash
# emulatorを起動
docker-compose up -d

# emulatorの準備（初回のみ必要）
./test-local.sh --keep-running  # または make test-local-keep

# その後は環境変数なしでテスト実行可能
go test -v ./integration_test.go
# または
make test-integration-only
```

emulatorが起動していない場合、テストは自動的にスキップされます。

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

## 設定のカスタマイズ

デフォルト値を変更したい場合は環境変数で上書きできます：

```bash
# デフォルト値の確認
export SPANNER_EMULATOR_HOST=localhost:9010  # 必須（emulator接続用）
# SPANNER_PROJECT_ID=test-project             # デフォルト値
# SPANNER_INSTANCE_ID=test-instance           # デフォルト値  
# SPANNER_DATABASE_ID=test-database           # デフォルト値

# カスタム値での実行例
SPANNER_PROJECT_ID=my-test ./test-local.sh
```

## トラブルシューティング

### Integration testsがスキップされる

```bash
# SPANNER_EMULATOR_HOSTが設定されているか確認
echo $SPANNER_EMULATOR_HOST

# 手動で設定して実行
SPANNER_EMULATOR_HOST=localhost:9010 go test -v ./integration_test.go
```

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