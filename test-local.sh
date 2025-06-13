#!/bin/bash

# Spanner emulator を使ったローカル統合テスト実行スクリプト

set -e

# 色付きの出力用
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 関数: メッセージ出力
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 関数: Docker Composeが利用可能かチェック
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
        log_error "Docker or docker-compose is not installed"
        exit 1
    fi
    
    # docker composeコマンドが利用可能かチェック（新しいDocker CLI）
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
    elif docker-compose --version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    else
        log_error "Neither 'docker compose' nor 'docker-compose' is available"
        exit 1
    fi
}

# 関数: gcloudが利用可能かチェック
check_gcloud() {
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first:"
        log_error "https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
}

# 関数: Spanner emulatorを起動
start_emulator() {
    log_info "Starting Spanner emulator..."
    
    $DOCKER_COMPOSE_CMD up -d spanner-emulator
    
    log_info "Waiting for Spanner emulator to be ready..."
    
    # ヘルスチェック待機
    timeout=30
    counter=0
    while [ $counter -lt $timeout ]; do
        if nc -z localhost 9010 && nc -z localhost 9020; then
            log_success "Spanner emulator is ready!"
            sleep 2  # 少し待ってから次のステップへ
            return 0
        fi
        sleep 1
        counter=$((counter + 1))
        if [ $((counter % 5)) -eq 0 ]; then
            log_info "Still waiting for emulator... (${counter}s)"
        fi
    done
    
    log_error "Timeout waiting for Spanner emulator to start"
    $DOCKER_COMPOSE_CMD logs spanner-emulator
    exit 1
}

# 関数: Spanner instance/databaseのセットアップ
setup_spanner() {
    log_info "Setting up Spanner instance and database..."
    
    # gcloud設定
    export CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER=http://localhost:9020/
    export SPANNER_EMULATOR_HOST=localhost:9010
    export SPANNER_EMULATOR_HOST_REST=localhost:9020
    export CLOUDSDK_CORE_PROJECT=test-project
    export CLOUDSDK_AUTH_DISABLE_CREDENTIALS=true
    
    gcloud config set project test-project
    gcloud config set auth/disable_credentials true
    gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
    
    # Instance作成
    if ! gcloud spanner instances describe test-instance 2>/dev/null; then
        log_info "Creating Spanner instance..."
        gcloud spanner instances create test-instance \
            --config=emulator-config \
            --description="Test instance for spannerdef" \
            --nodes=1
    else
        log_info "Spanner instance already exists"
    fi
    
    # Database作成
    if ! gcloud spanner databases describe test-database --instance=test-instance 2>/dev/null; then
        log_info "Creating Spanner database..."
        gcloud spanner databases create test-database \
            --instance=test-instance
    else
        log_info "Spanner database already exists"
    fi
    
    log_success "Spanner setup completed"
}

# 関数: テスト実行
run_tests() {
    log_info "Running tests..."
    
    # 環境変数設定
    export CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER=http://localhost:9020/
    export SPANNER_EMULATOR_HOST=localhost:9010
    export SPANNER_EMULATOR_HOST_REST=localhost:9020
    
    echo ""
    log_info "Running unit tests..."
    if go test -v ./...; then
        log_success "Unit tests passed"
    else
        log_error "Unit tests failed"
        return 1
    fi
    
    echo ""
    log_info "Running integration tests..."
    if go test -v ./integration_test.go; then
        log_success "Integration tests passed"
    else
        log_error "Integration tests failed"
        return 1
    fi
    
    echo ""
    log_success "All tests passed! 🎉"
}

# 関数: クリーンアップ
cleanup() {
    if [ "$KEEP_RUNNING" != "1" ]; then
        log_info "Stopping Spanner emulator..."
        $DOCKER_COMPOSE_CMD down
        log_success "Cleanup completed"
    else
        log_info "Keeping Spanner emulator running (use --stop to stop it)"
    fi
}

# 関数: ヘルプ表示
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --keep-running    Keep Spanner emulator running after tests"
    echo "  --stop           Stop Spanner emulator"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                     # Run tests and stop emulator"
    echo "  $0 --keep-running      # Run tests and keep emulator running"
    echo "  $0 --stop              # Stop running emulator"
}

# メイン処理
main() {
    # オプション解析
    while [[ $# -gt 0 ]]; do
        case $1 in
            --keep-running)
                KEEP_RUNNING=1
                shift
                ;;
            --stop)
                check_docker_compose
                log_info "Stopping Spanner emulator..."
                $DOCKER_COMPOSE_CMD down
                log_success "Spanner emulator stopped"
                exit 0
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    log_info "Starting spannerdef integration tests..."
    
    # 依存関係チェック
    check_docker_compose
    check_gcloud
    
    # クリーンアップハンドラ設定
    trap cleanup EXIT
    
    # メイン処理
    start_emulator
    setup_spanner
    run_tests
}

# エラーハンドリング
set -E
trap 'log_error "Script failed at line $LINENO"' ERR

# スクリプト実行
main "$@"