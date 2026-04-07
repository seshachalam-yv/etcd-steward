TOOLS_DIR                  := hack/tools
TOOLS_BIN_DIR              := $(TOOLS_DIR)/bin
GOLANGCI_LINT              := $(TOOLS_BIN_DIR)/golangci-lint
GO_ADD_LICENSE             := $(TOOLS_BIN_DIR)/addlicense

GOLANGCI_LINT_VERSION ?= v2.6.2
GO_ADD_LICENSE_VERSION ?= latest

export TOOLS_BIN_DIR := $(TOOLS_BIN_DIR)
export PATH := $(abspath $(TOOLS_BIN_DIR)):$(PATH)

define tool_version_file
$(TOOLS_BIN_DIR)/.version_$(notdir $1)_$2
endef

#########################################
# Tools                                 #
#########################################

$(GOLANGCI_LINT): $(call tool_version_file,$(GOLANGCI_LINT),$(GOLANGCI_LINT_VERSION))
	@mkdir -p $(TOOLS_BIN_DIR)
	GOBIN=$(abspath $(TOOLS_BIN_DIR)) CGO_ENABLED=1 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@touch $@

$(GO_ADD_LICENSE):
	@mkdir -p $(TOOLS_BIN_DIR)
	GOBIN=$(abspath $(TOOLS_BIN_DIR)) go install github.com/google/addlicense@$(GO_ADD_LICENSE_VERSION)

$(call tool_version_file,$(GOLANGCI_LINT),$(GOLANGCI_LINT_VERSION)):
	@mkdir -p $(TOOLS_BIN_DIR)
	@rm -f $(TOOLS_BIN_DIR)/.version_$(notdir $(GOLANGCI_LINT))_*
	@touch $@
