.PHONY: all build clean test bench install run help docker

VERSION := 1.0.0
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT)
IMAGE_REPO := grumpylabs
IMAGE_NAME := gopogo
IMAGE_TAG := $(IMAGE_NAME):$(VERSION)

RUN_ARGS ?=
.DEFAULT_GOAL := help

all: build ## Build the project

build: ## Build the binary
	@go build -ldflags "$(LDFLAGS)" -o bin/gopogo cmd/main.go

build-race: ## Build with race detector enabled
	@go build -race -ldflags "$(LDFLAGS)" -o bin/gopogo-race cmd/main.go

clean: ## Clean build artifacts and cache
	@rm -rf bin/
	@rm -f .docker-build-*
	@go clean -cache

test: ## Run all tests with race detection
	@go test -v -race -cover ./...

test-coverage: ## Run tests and generate coverage report
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench: ## Run performance benchmarks
	@go test -bench=. -benchmem ./...

fmt: ## Format Go source code
	@go fmt ./...
	@gofmt -s -w .

vet: ## Run go vet static analysis
	@go vet ./...

lint: ## Run revive linter (auto-installs if needed)
	@command -v revive >/dev/null 2>&1 || { echo "Installing revive..."; go install github.com/mgechev/revive@latest; }
	@revive -config revive.toml -formatter friendly ./...

deps: ## Download and tidy Go modules
	@go mod download
	@go mod tidy

local: build ## Build and start the server
	@./bin/gopogo

.docker-build-$(VERSION): Dockerfile $(shell find . -name "*.go" -o -name "go.mod" -o -name "go.sum")
	@docker build -t $(IMAGE_REPO)/$(IMAGE_NAME):$(VERSION) .
	@touch .docker-build-$(VERSION)

docker: .docker-build-$(VERSION) ## Build Docker image

run: .docker-build-$(VERSION) ## Run server in Docker container
	@docker run -p 6379:6379 -p 8080:8080 $(IMAGE_REPO)/$(IMAGE_NAME):$(VERSION) $(RUN_ARGS)

push: .docker-build-$(VERSION) ## Push Docker image to registry
	@docker push $(IMAGE_REPO)/$(IMAGE_NAME):$(VERSION)

profile-cpu: ## Profile CPU usage and open pprof
	@go test -cpuprofile=cpu.prof -bench=. ./internal/cache
	@go tool pprof cpu.prof

profile-mem: ## Profile memory usage and open pprof
	@echo "Profiling memory..."
	@go test -memprofile=mem.prof -bench=. ./internal/cache
	@go tool pprof mem.prof

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | \
		sort
