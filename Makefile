.PHONY: build test omni-up omni-down clean help

# Spanner Omni single-server defaults
OMNI_HOST      ?= localhost:15000
OMNI_PROJECT   ?= default
OMNI_INSTANCE  ?= default
OMNI_IMAGE     ?= us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r1-beta
OMNI_CONTAINER ?= spanneromni

# -parallel 80 empirically minimises wall time against Omni single-server
# on an 8-core laptop (~80s vs ~150s at -parallel 10). Going higher starts
# to hurt as Omni serializes CREATE DATABASE internally.
TEST_PARALLEL ?= 80

build:
	go build -o bin/spannerdef ./cmd/spannerdef

# Run tests against Spanner Omni (start it with `make omni-up` first).
# -count=1 disables Go's test cache — integration tests depend on the live
# Omni server so we always want them to actually run.
test:
	SPANNER_EMULATOR_HOST=$(OMNI_HOST) \
	SPANNER_PROJECT_ID=$(OMNI_PROJECT) \
	SPANNER_INSTANCE_ID=$(OMNI_INSTANCE) \
	go test -v -count=1 -timeout 15m -parallel $(TEST_PARALLEL) ./...

omni-up:
	docker run -d --network host --name $(OMNI_CONTAINER) \
		-v spanner:/spanner $(OMNI_IMAGE) start-single-server
	@echo "Waiting for Spanner Omni to be ready..."
	@until docker logs $(OMNI_CONTAINER) 2>&1 | grep -q "Spanner is ready"; do sleep 2; done
	@echo "Spanner Omni is ready on $(OMNI_HOST)"

omni-down:
	-docker rm -f $(OMNI_CONTAINER)
	-docker volume rm spanner

clean:
	rm -rf bin/

help:
	@echo "Available targets:"
	@echo "  build      - Build spannerdef binary"
	@echo "  test       - Run tests against Spanner Omni"
	@echo "  omni-up    - Start Spanner Omni container"
	@echo "  omni-down  - Stop Spanner Omni and clear its volume"
	@echo "  clean      - Clean build artifacts"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "To run tests with Spanner Omni:"
	@echo "  make omni-up   # Start Omni"
	@echo "  make test      # Run tests"
