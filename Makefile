.PHONY: build test test-unit test-integration test-local test-local-keep clean help

build:
	go build -o bin/spannerdef ./cmd/spannerdef

test: test-unit
	@echo "Use 'make test-local' to run integration tests with Spanner emulator"

test-unit:
	go test ./...

test-integration:
	@echo "Starting integration tests with Spanner emulator..."
	./test-local.sh

test-local: test-integration

test-local-keep:
	@echo "Starting integration tests with Spanner emulator (keeping it running)..."
	./test-local.sh --keep-running

test-stop:
	@echo "Stopping Spanner emulator..."
	./test-local.sh --stop

clean:
	rm -rf bin/

# Legacy setup command (for compatibility)
setup-emulator:
	curl -s "${SPANNER_EMULATOR_HOST_REST}/v1/projects/${SPANNER_PROJECT_ID}/instances" --data '{"instanceId": "'${SPANNER_INSTANCE_ID}'"}'
	curl -s "${SPANNER_EMULATOR_HOST_REST}/v1/projects/${SPANNER_PROJECT_ID}/instances/${SPANNER_INSTANCE_ID}/databases" --data '{"createStatement": "CREATE DATABASE `'${SPANNER_DATABASE_ID}'`"}'

help:
	@echo "Available targets:"
	@echo "  build            - Build spannerdef binary"
	@echo "  test             - Run unit tests only"
	@echo "  test-unit        - Run unit tests only"
	@echo "  test-local       - Run all tests with Spanner emulator (start & stop)"
	@echo "  test-integration - Same as test-local"
	@echo "  test-local-keep  - Run tests but keep emulator running"
	@echo "  test-stop        - Stop running Spanner emulator"
	@echo "  clean            - Clean build artifacts"
	@echo "  help             - Show this help message"
