.PHONY: all build loadgen amd64 arm64 clean test integration integration-run helm-lint bench install run run-ports help container login dock-amd64 dock-arm64 dev-manifest dev images ci-pkg-amd64 ci-pkg-arm64 ci-pkg push

# Version stamped into the binary: the nearest v* git tag (v1.2.3 -> 1.2.3),
# with -<n>-g<commit> after it and -dirty for uncommitted changes.
VERSION := $(or $(shell git describe --tags --always --dirty --match 'v*' 2>/dev/null | sed 's/^v//'),dev)
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT)
# Container images: each arch's static binary is built natively with
# `make amd64` / `make arm64`, packaged by
# a plain `docker build` as <image>:<commit>-<arch>, and the arch images are
# joined into multi-arch tags with `docker manifest`. No buildx or emulation.
REPO := ghcr.io/grumpylabs
IMAGE := $(REPO)/gopogo
IMAGE_AMD_TAG := $(IMAGE):$(COMMIT)-amd64
IMAGE_ARM_TAG := $(IMAGE):$(COMMIT)-arm64
IMAGE_TAG := $(IMAGE):$(COMMIT)
IMAGE_LATEST := $(IMAGE):latest
# When HEAD is exactly a v* tag (v1.2.3), ci-pkg also publishes :1.2.3.
RELEASE ?= $(shell git describe --tags --exact-match --match 'v*' 2>/dev/null | sed 's/^v//')

# Developer images: <image>:<user>-<commit>[-<arch>]
IMAGE_DEV_USER := $(IMAGE):$(USER)-$(COMMIT)
IMAGE_AMD_DEV := $(IMAGE_DEV_USER)-amd64
IMAGE_ARM_DEV := $(IMAGE_DEV_USER)-arm64

# Local image for `make container` / `make run`, built for this machine's arch.
ARCH ?= $(shell go env GOARCH)
LOCAL_IMAGE := $(IMAGE):dev

RUN_ARGS ?=
.DEFAULT_GOAL := help

all: build ## Build the project

build: ## Build the binary
	@go build -trimpath -ldflags "$(LDFLAGS)" -o bin/gopogo ./cmd

amd64: ## Build static linux/amd64 binaries (bin/gopogo-amd64, bin/gopogo-loadgen-amd64)
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/gopogo-amd64 ./cmd
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o bin/gopogo-loadgen-amd64 ./cmd/loadgen

arm64: ## Build static linux/arm64 binaries (bin/gopogo-arm64, bin/gopogo-loadgen-arm64)
	@CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/gopogo-arm64 ./cmd
	@CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -o bin/gopogo-loadgen-arm64 ./cmd/loadgen

loadgen: ## Build the load generator (bin/gopogo-loadgen)
	@go build -trimpath -o bin/gopogo-loadgen ./cmd/loadgen

build-race: ## Build with race detector enabled
	@go build -trimpath -race -ldflags "$(LDFLAGS)" -o bin/gopogo-race ./cmd

clean: ## Clean build artifacts and cache
	@rm -rf bin/
	@go clean -cache

test: ## Run all tests with race detection
	@go test -v -race -cover ./...

INTEGRATION_RUN ?= .
INTEGRATION_SKIP ?=

integration: build integration-run ## Run pogocache's protocol tests against a live server

integration-run: ## Run the integration tests without rebuilding (GOPOGO_BIN overrides the binary)
	@test/integration/run.sh -timeout 5m -run '$(INTEGRATION_RUN)' $(if $(INTEGRATION_SKIP),-skip '$(INTEGRATION_SKIP)')

helm-lint: ## Lint the Helm chart
	@helm lint --strict deploy/helm/gopogo

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

container: $(ARCH) ## Build the local image $(IMAGE):dev for this machine's arch
	@docker build . -t $(LOCAL_IMAGE) -f Dockerfile --platform=linux/$(ARCH) \
		--build-arg TARGETARCH=$(ARCH) --provenance=false

run: container ## Run server in a container
	@docker run --net=host $(LOCAL_IMAGE) $(RUN_ARGS)

run-ports: container ## Run server in a container with port mapping
	@docker run -p 6379:6379 -p 8080:8080 -p 11211:11211 -p 5432:5432 $(LOCAL_IMAGE) $(RUN_ARGS)

login: ## Log in to ghcr.io (GHCR_TOKEN/GHCR_USER, default: gh auth token and gh user)
	@echo "$${GHCR_TOKEN:-$$(gh auth token)}" | docker login ghcr.io \
		-u "$${GHCR_USER:-$$(gh api user -q .login)}" --password-stdin

# Developer builds: build and push <image>:<user>-<commit> for both arches.
dock-amd64: amd64 login
	docker build . -t $(IMAGE_AMD_DEV) -f Dockerfile --platform=linux/amd64 \
		--build-arg TARGETARCH=amd64 --provenance=false \
		&& docker push $(IMAGE_AMD_DEV)

dock-arm64: arm64 login
	docker build . -t $(IMAGE_ARM_DEV) -f Dockerfile --platform=linux/arm64 \
		--build-arg TARGETARCH=arm64 --provenance=false \
		&& docker push $(IMAGE_ARM_DEV)

dev-manifest: login
	docker manifest rm $(IMAGE_DEV_USER) || true
	docker manifest create $(IMAGE_DEV_USER) $(IMAGE_AMD_DEV) $(IMAGE_ARM_DEV)
	docker manifest push $(IMAGE_DEV_USER)

dev: dock-amd64 dock-arm64 dev-manifest ## Build and push dev images $(IMAGE):<user>-<commit>
	@echo "Built and pushed dev images: $(IMAGE_DEV_USER) (amd64 + arm64)"

# CI builds: each arch job runs `make <arch> ci-pkg-<arch>` on a native runner,
# then one job runs `make ci-pkg` to publish the multi-arch tags. `make images`
# builds the same per-arch images locally without pushing; `make push` runs the
# whole CI sequence from this machine.
images: amd64 arm64 ## Build the CI images $(IMAGE):$(COMMIT)-<arch> locally, without pushing
	docker build . -t $(IMAGE_AMD_TAG) -f Dockerfile --platform=linux/amd64 \
		--build-arg TARGETARCH=amd64 --provenance=false
	docker build . -t $(IMAGE_ARM_TAG) -f Dockerfile --platform=linux/arm64 \
		--build-arg TARGETARCH=arm64 --provenance=false

ci-pkg-amd64: login
	docker build . -t $(IMAGE_AMD_TAG) -f Dockerfile --platform=linux/amd64 \
		--build-arg TARGETARCH=amd64 --provenance=false
	docker push $(IMAGE_AMD_TAG)

ci-pkg-arm64: login
	docker build . -t $(IMAGE_ARM_TAG) -f Dockerfile --platform=linux/arm64 \
		--build-arg TARGETARCH=arm64 --provenance=false
	docker push $(IMAGE_ARM_TAG)

ci-pkg: login
	@for tag in $(IMAGE_TAG) $(IMAGE_LATEST) $(if $(RELEASE),$(IMAGE):$(RELEASE)); do \
		docker manifest rm $$tag 2>/dev/null || true; \
		docker manifest create $$tag $(IMAGE_AMD_TAG) $(IMAGE_ARM_TAG) && \
		docker manifest push $$tag || exit 1; \
	done

push: amd64 arm64 ci-pkg-amd64 ci-pkg-arm64 ci-pkg ## Build and push $(IMAGE):$(COMMIT), :latest and :$(RELEASE) if tagged
	@echo "$$(date) : Built and pushed image - $(IMAGE_TAG)"

profile-cpu: ## Profile CPU usage and open pprof
	@go test -cpuprofile=cpu.prof -bench=. ./internal/cache
	@go tool pprof cpu.prof

profile-mem: ## Profile memory usage and open pprof
	@echo "Profiling memory..."
	@go test -memprofile=mem.prof -bench=. ./internal/cache
	@go tool pprof mem.prof

install: build ## Install the binary to /usr/local/bin
	@install -d /usr/local/bin
	@install -m 755 bin/gopogo /usr/local/bin/gopogo
	@echo "gopogo installed to /usr/local/bin/gopogo"

help: ## Show help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | \
		sort
