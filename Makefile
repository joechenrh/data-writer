.PHONY: build build_debug clean

BIN_DIR := bin
SRC_DIR := src
TARGET := $(BIN_DIR)/parquet-writer

build: $(TARGET)

$(TARGET): $(SRC_DIR)/*
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=1 go build -o $(TARGET) ./$(SRC_DIR)

build_debug:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=1 go build -gcflags="all=-N -l" -o $(TARGET) ./$(SRC_DIR)

clean:
	rm -rf $(BIN_DIR)
