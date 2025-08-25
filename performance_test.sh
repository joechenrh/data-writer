#!/bin/bash

# Performance Comparison Script
# Compares the current lock-free implementation with the original implementation

set -e

REPO_DIR="/home/runner/work/data-writer/data-writer"
RESULTS_DIR="/tmp/benchmark_results"
CURRENT_COMMIT="1bb0e7d"
ORIGINAL_COMMIT="3f474d4"

cd "$REPO_DIR"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "=== Performance Comparison: Current vs Original Implementation ==="
echo "Current commit: $CURRENT_COMMIT"
echo "Original commit: $ORIGINAL_COMMIT"
echo "Results will be saved to: $RESULTS_DIR"
echo ""

# Function to run benchmarks and save results
run_benchmarks() {
    local version="$1"
    local commit="$2"
    
    echo "Running benchmarks for $version implementation (commit: $commit)..."
    
    # Build the application
    cd src
    go build -o ../data-writer . || {
        echo "Build failed for $version implementation"
        return 1
    }
    cd ..
    
    # Run benchmarks with various configurations
    cd src
    go test -bench=BenchmarkComparison -benchmem -count=3 -timeout=30m > "$RESULTS_DIR/${version}_benchmark.txt" 2>&1 || {
        echo "Benchmark failed for $version implementation"
        return 1
    }
    cd ..
    
    echo "$version benchmarks completed successfully"
}

# Function to create simple performance test
create_simple_test() {
    local version="$1"
    
    echo "Creating simple performance test for $version..."
    
    # Create a simple test configuration
    cat > /tmp/simple_test.toml << EOF
[common]
spec_file = "/tmp/simple_spec.toml"
output_dir = "/tmp/output_$version"
use_streaming_mode = true
chunk_size_kb = 64

EOF

    # Create a simple spec
    cat > /tmp/simple_spec.toml << EOF
[[files]]
name = "test_file_1.parquet"
format = "parquet"
rows = 10000

[[files.columns]]
name = "id"
type = "int"
min = 1
max = 1000000

[[files.columns]]
name = "name"
type = "string"
length = 20

[[files.columns]]
name = "email"  
type = "string"
length = 30

[[files.columns]]
name = "age"
type = "int"
min = 18
max = 80

[[files.columns]]
name = "score"
type = "float"
min = 0.0
max = 100.0

[[files]]
name = "test_file_2.parquet"
format = "parquet"
rows = 10000

[[files.columns]]
name = "id"
type = "int"
min = 1
max = 1000000

[[files.columns]]
name = "name"
type = "string"
length = 20

[[files.columns]]
name = "email"
type = "string"
length = 30

[[files.columns]]
name = "age"
type = "int"
min = 18
max = 80

[[files.columns]]
name = "score"
type = "float"
min = 0.0
max = 100.0

[[files]]
name = "test_file_3.csv"
format = "csv"
rows = 10000

[[files.columns]]
name = "id"
type = "int"
min = 1
max = 1000000

[[files.columns]]
name = "name"
type = "string"
length = 20

[[files.columns]]
name = "email"
type = "string"
length = 30

EOF

    # Clean output directory
    rm -rf "/tmp/output_$version"
    mkdir -p "/tmp/output_$version"
}

# Function to run simple performance test
run_simple_test() {
    local version="$1"
    
    echo "Running simple performance test for $version..."
    
    create_simple_test "$version"
    
    # Time the execution
    echo "Timing data generation for $version implementation..."
    /usr/bin/time -v ./data-writer /tmp/simple_test.toml > "$RESULTS_DIR/${version}_simple_output.txt" 2> "$RESULTS_DIR/${version}_simple_time.txt"
    
    # Record file sizes
    echo "File sizes for $version:" > "$RESULTS_DIR/${version}_file_sizes.txt"
    du -h "/tmp/output_$version"/* >> "$RESULTS_DIR/${version}_file_sizes.txt" 2>/dev/null || echo "No files generated" >> "$RESULTS_DIR/${version}_file_sizes.txt"
    
    # Record memory usage and timing
    echo "$version performance test completed"
}

# Save current state
echo "Saving current state..."
git stash push -m "benchmark: saving current changes"

# Test current implementation
echo ""
echo "=== Testing Current Implementation ==="
run_simple_test "current"

# Checkout original implementation  
echo ""
echo "=== Switching to Original Implementation ==="
echo "Checking out original commit: $ORIGINAL_COMMIT"
git checkout "$ORIGINAL_COMMIT" -- src/ || {
    echo "Failed to checkout original implementation"
    git stash pop
    exit 1
}

# Handle configuration differences for original
echo "Adapting configuration for original implementation..."
if [ -f "src/config.go" ]; then
    # Check if streaming mode exists in original
    if ! grep -q "UseStreamingMode" src/config.go; then
        echo "Original implementation doesn't support streaming mode"
        # Modify the test config for original implementation
        cat > /tmp/simple_test.toml << EOF
[common]
spec_file = "/tmp/simple_spec.toml"
output_dir = "/tmp/output_original"

EOF
    fi
fi

# Test original implementation
echo ""
echo "=== Testing Original Implementation ==="
run_simple_test "original"

# Restore current implementation
echo ""
echo "=== Restoring Current Implementation ==="
git stash pop

# Generate comparison report
echo ""
echo "=== Generating Performance Report ==="

cat > "$RESULTS_DIR/performance_report.md" << EOF
# Performance Comparison Report

## Test Configuration
- Current Implementation Commit: $CURRENT_COMMIT (Lock-free coordinator with paired generator-writer goroutines)
- Original Implementation Commit: $ORIGINAL_COMMIT (Original implementation)
- Test Files: 2 Parquet files (10,000 rows each) + 1 CSV file (10,000 rows)
- Test Environment: $(uname -a)

## Timing Results

### Current Implementation
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/current_simple_time.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No timing data available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

### Original Implementation
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/original_simple_time.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No timing data available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

## File Size Comparison

### Current Implementation
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/current_file_sizes.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No file size data available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

### Original Implementation  
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/original_file_sizes.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No file size data available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

# Extract key metrics for summary
extract_metric() {
    local file="$1"
    local metric="$2"
    grep "$metric" "$file" 2>/dev/null | head -1 || echo "$metric: Not available"
}

cat >> "$RESULTS_DIR/performance_report.md" << EOF

## Performance Summary

| Metric | Current Implementation | Original Implementation | Improvement |
|--------|----------------------|------------------------|-------------|
EOF

# Extract elapsed time
current_time=$(extract_metric "$RESULTS_DIR/current_simple_time.txt" "Elapsed.*time.*:" | grep -o '[0-9]*:[0-9]*\.[0-9]*' | head -1)
original_time=$(extract_metric "$RESULTS_DIR/original_simple_time.txt" "Elapsed.*time.*:" | grep -o '[0-9]*:[0-9]*\.[0-9]*' | head -1)

# Extract memory usage
current_memory=$(extract_metric "$RESULTS_DIR/current_simple_time.txt" "Maximum resident set size" | grep -o '[0-9]*' | head -1)
original_memory=$(extract_metric "$RESULTS_DIR/original_simple_time.txt" "Maximum resident set size" | grep -o '[0-9]*' | head -1)

echo "| Elapsed Time | ${current_time:-N/A} | ${original_time:-N/A} | TBD |" >> "$RESULTS_DIR/performance_report.md"
echo "| Peak Memory (KB) | ${current_memory:-N/A} | ${original_memory:-N/A} | TBD |" >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

## Detailed Output Logs

### Current Implementation Output
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/current_simple_output.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No output available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

### Original Implementation Output
EOF

echo '```' >> "$RESULTS_DIR/performance_report.md"
cat "$RESULTS_DIR/original_simple_output.txt" >> "$RESULTS_DIR/performance_report.md" 2>/dev/null || echo "No output available" >> "$RESULTS_DIR/performance_report.md"
echo '```' >> "$RESULTS_DIR/performance_report.md"

echo ""
echo "=== Performance Comparison Complete ==="
echo "Results saved to: $RESULTS_DIR/performance_report.md"
echo ""
echo "Summary:"
echo "- Current implementation: Lock-free coordinator with paired generator-writer goroutines"
echo "- Original implementation: Traditional mutex-based coordination"
echo "- Report available at: $RESULTS_DIR/performance_report.md"

# Display the report
echo ""
echo "=== Performance Report ==="
cat "$RESULTS_DIR/performance_report.md"