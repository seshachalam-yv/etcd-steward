BIN_DIR := bin

include hack/tools.mk

.PHONY: build
build:
	CGO_ENABLED=0 go build -o $(BIN_DIR)/etcd-steward ./cmd/etcdsteward/

.PHONY: test-unit
test-unit:
	go test -count=1 -race ./internal/... ./cmd/...

.PHONY: test-integration
test-integration:
	go test -count=1 -race -tags=integration ./test/integration/...

.PHONY: test-e2e
test-e2e:
	go test -count=1 -race -tags=e2e ./test/e2e/...

.PHONY: check
check: $(GOLANGCI_LINT)
	@./hack/check.sh --golangci-lint-config=./.golangci.yaml ./internal/... ./cmd/...

.PHONY: verify
verify: check test-unit

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR)/

.PHONY: revendor
revendor:
	@env GO111MODULE=on go mod tidy
	@env GO111MODULE=on go mod vendor

.PHONY: add-license-headers
add-license-headers: $(GO_ADD_LICENSE)
	@./hack/add_license_headers.sh
