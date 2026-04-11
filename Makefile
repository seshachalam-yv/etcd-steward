BIN_DIR := bin

include hack/tools.mk

.PHONY: build
build:
	@CGO_ENABLED=0 go build -mod vendor -o $(BIN_DIR)/etcd-steward ./cmd/etcd-steward/

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR)/

.PHONY: revendor
revendor:
	@env GO111MODULE=on go mod tidy
	@env GO111MODULE=on go mod vendor

.PHONY: check
check: $(GOLANGCI_LINT)
	@./hack/check.sh --golangci-lint-config=./.golangci.yaml ./pkg/... ./cmd/...

.PHONY: test
test:
	@go test -count=1 -race ./pkg/... ./cmd/...

.PHONY: test-integration
test-integration:
	@go test -count=1 -tags integration -v ./pkg/snapstore/...

.PHONY: add-license-headers
add-license-headers: $(GO_ADD_LICENSE)
	@./hack/add_license_headers.sh
