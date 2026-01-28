.PHONY: build build_debug clean

BIN_DIR := bin
SRC_DIR := src
TARGET := $(BIN_DIR)/data-writer
GO_SOURCES := $(shell find $(SRC_DIR) -type f -name '*.go')
GO_MODS := go.mod go.sum

build: $(TARGET)

$(BIN_DIR):
	@mkdir -p $@

$(TARGET): $(GO_SOURCES) $(GO_MODS) | $(BIN_DIR)
	CGO_ENABLED=1 go build -o $(TARGET) ./$(SRC_DIR)

build_debug: $(GO_SOURCES) $(GO_MODS) | $(BIN_DIR)
	CGO_ENABLED=1 go build -gcflags="all=-N -l" -o $(TARGET) ./$(SRC_DIR)

clean:
	rm -rf $(BIN_DIR)
