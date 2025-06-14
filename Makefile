.PHONY: build test clean help

build:
	go build -o bin/spannerdef ./cmd/spannerdef

test:
	go test -v ./...

clean:
	rm -rf bin/

help:
	@echo "Available targets:"
	@echo "  build        - Build spannerdef binary"
	@echo "  test         - Run all tests (requires emulator for some tests)"
	@echo "  clean        - Clean build artifacts"
	@echo "  help         - Show this help message"
	@echo ""
	@echo "To run tests with Spanner emulator:"
	@echo "  docker-compose up -d  # Start emulator"
	@echo "  make test             # Run all tests"
