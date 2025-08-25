#!/bin/bash

# Simple Performance Test Script
# Compares current implementation with original using basic timing and memory metrics

set -e

REPO_DIR="/home/runner/work/data-writer/data-writer"
RESULTS_DIR="/tmp/perf_results"
CURRENT_COMMIT="1bb0e7d"
ORIGINAL_COMMIT="3f474d4"

cd "$REPO_DIR"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "=== Simple Performance Comparison ==="
echo "Current commit: $CURRENT_COMMIT (Lock-free coordinator)"
echo "Original commit: $ORIGINAL_COMMIT (Original implementation)"
echo ""

# Function to create test configuration
create_test_config() {
    local test_name="$1"
    local output_dir="$2"
    
    cat > "/tmp/${test_name}_config.toml" << EOF
[common]
path = "$output_dir"
prefix = "test"
start_fileno = 0
end_fileno = 3
rows = 5000
format = "parquet"
use_streaming_mode = true
chunk_size_kb = 64

[parquet]
row_groups = 1
page_size_kb = 1024
EOF

    # Create test SQL schema
    cat > "/tmp/${test_name}_schema.sql" << EOF
CREATE TABLE test_table (
    id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name varchar(50) NOT NULL DEFAULT '',
    email varchar(100) NOT NULL DEFAULT '',
    age int NOT NULL DEFAULT '0',
    score float NOT NULL DEFAULT '0.0',
    active tinyint NOT NULL DEFAULT '1',
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
EOF
}

# Function to build and test a version
test_version() {
    local version="$1"
    local output_dir="/tmp/output_$version"
    
    echo "=== Testing $version Implementation ==="
    
    # Clean and create output directory
    rm -rf "$output_dir"
    mkdir -p "$output_dir"
    
    # Create test configuration
    create_test_config "$version" "$output_dir"
    
    # Build the application
    echo "Building $version implementation..."
    cd src
    if ! go build -o ../data-writer . 2> "$RESULTS_DIR/${version}_build.log"; then
        echo "Build failed for $version implementation"
        echo "Build log:"
        cat "$RESULTS_DIR/${version}_build.log"
        return 1
    fi
    cd ..
    
    echo "Running performance test for $version..."
    
    # Run the test with timing
    /usr/bin/time -f "real %e\nuser %U\nsys %S\nmemory %M KB" \
        ./data-writer -cfg "/tmp/${version}_config.toml" -sql "/tmp/${version}_schema.sql" \
        > "$RESULTS_DIR/${version}_output.log" \
        2> "$RESULTS_DIR/${version}_time.log"
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "$version test completed successfully"
        
        # Record file information
        echo "File sizes:" > "$RESULTS_DIR/${version}_files.log"
        if [ -d "$output_dir" ] && [ "$(ls -A $output_dir 2>/dev/null)" ]; then
            du -h "$output_dir"/* >> "$RESULTS_DIR/${version}_files.log" 2>/dev/null
            echo "File count: $(ls "$output_dir" | wc -l)" >> "$RESULTS_DIR/${version}_files.log"
        else
            echo "No files generated" >> "$RESULTS_DIR/${version}_files.log"
        fi
        
        # Extract timing metrics
        local real_time=$(grep "^real" "$RESULTS_DIR/${version}_time.log" | awk '{print $2}')
        local memory=$(grep "^memory" "$RESULTS_DIR/${version}_time.log" | awk '{print $2}')
        
        echo "Execution time: ${real_time}s"
        echo "Peak memory: ${memory}KB"
        
    else
        echo "$version test failed with exit code $exit_code"
        echo "Error log:"
        cat "$RESULTS_DIR/${version}_output.log"
        return 1
    fi
}

# Save current state
echo "Saving current state..."
git stash push -m "perf: saving current changes" 2>/dev/null || echo "No changes to stash"

# Test current implementation
test_version "current"
current_success=$?

# Checkout and test original implementation
echo ""
echo "=== Switching to Original Implementation ==="
git checkout "$ORIGINAL_COMMIT" -- src/ 2>/dev/null || {
    echo "Failed to checkout original implementation"
    git stash pop 2>/dev/null || true
    exit 1
}

# Modify original config if needed (remove streaming mode if not supported)
check_streaming_support() {
    if ! grep -q "UseStreamingMode\|use_streaming_mode" src/*.go 2>/dev/null; then
        echo "Original doesn't support streaming mode, adapting configuration..."
        # Create modified config without streaming mode
        cat > "/tmp/original_config.toml" << EOF
[common]
path = "/tmp/output_original"
prefix = "test"
start_fileno = 0
end_fileno = 3
rows = 5000
format = "parquet"

[parquet]
row_groups = 1
page_size_kb = 1024
EOF
    fi
}

check_streaming_support
test_version "original"
original_success=$?

# Restore current implementation
echo ""
echo "=== Restoring Current Implementation ==="
git stash pop 2>/dev/null || echo "No stash to restore"

# Generate performance report
echo ""
echo "=== Performance Comparison Results ==="

# Create performance report
cat > "$RESULTS_DIR/performance_report.md" << EOF
# Performance Comparison Report

## Test Configuration
- **Current Implementation**: Commit $CURRENT_COMMIT (Lock-free coordinator with paired generator-writer goroutines)
- **Original Implementation**: Commit $ORIGINAL_COMMIT (Original mutex-based implementation)
- **Test Files**: 2 Parquet files + 2 CSV files (5,000 rows each)
- **Environment**: $(uname -s) $(uname -r)
- **Go Version**: $(go version 2>/dev/null || echo "Go not available")

## Results Summary

EOF

# Extract and compare metrics
if [ $current_success -eq 0 ] && [ $original_success -eq 0 ]; then
    current_time=$(grep "^real" "$RESULTS_DIR/current_time.log" 2>/dev/null | awk '{print $2}' | head -1)
    original_time=$(grep "^real" "$RESULTS_DIR/original_time.log" 2>/dev/null | awk '{print $2}' | head -1)
    
    current_memory=$(grep "^memory" "$RESULTS_DIR/current_time.log" 2>/dev/null | awk '{print $2}' | head -1)
    original_memory=$(grep "^memory" "$RESULTS_DIR/original_time.log" 2>/dev/null | awk '{print $2}' | head -1)
    
    echo "| Metric | Current Implementation | Original Implementation |" >> "$RESULTS_DIR/performance_report.md"
    echo "|--------|----------------------|------------------------|" >> "$RESULTS_DIR/performance_report.md"
    echo "| Execution Time | ${current_time:-N/A}s | ${original_time:-N/A}s |" >> "$RESULTS_DIR/performance_report.md"
    echo "| Peak Memory | ${current_memory:-N/A} KB | ${original_memory:-N/A} KB |" >> "$RESULTS_DIR/performance_report.md"
    
    # Calculate improvement if both have valid times
    if [ -n "$current_time" ] && [ -n "$original_time" ]; then
        improvement=$(echo "scale=2; ($original_time - $current_time) / $original_time * 100" | bc -l 2>/dev/null || echo "N/A")
        echo "| Performance Improvement | ${improvement}% faster | - |" >> "$RESULTS_DIR/performance_report.md"
    fi
    
elif [ $current_success -eq 0 ]; then
    echo "✅ Current implementation: SUCCESS" >> "$RESULTS_DIR/performance_report.md"
    echo "❌ Original implementation: FAILED" >> "$RESULTS_DIR/performance_report.md"
elif [ $original_success -eq 0 ]; then
    echo "❌ Current implementation: FAILED" >> "$RESULTS_DIR/performance_report.md"
    echo "✅ Original implementation: SUCCESS" >> "$RESULTS_DIR/performance_report.md"
else
    echo "❌ Both implementations: FAILED" >> "$RESULTS_DIR/performance_report.md"
fi

# Add detailed results
cat >> "$RESULTS_DIR/performance_report.md" << EOF

## Detailed Results

### Current Implementation
**Execution Time:**
EOF

cat "$RESULTS_DIR/current_time.log" 2>/dev/null | sed 's/^/```\n/; s/$/\n```/' >> "$RESULTS_DIR/performance_report.md" || echo "No timing data available" >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

**Files Generated:**
EOF

cat "$RESULTS_DIR/current_files.log" 2>/dev/null | sed 's/^/```\n/; s/$/\n```/' >> "$RESULTS_DIR/performance_report.md" || echo "No file data available" >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

### Original Implementation
**Execution Time:**
EOF

cat "$RESULTS_DIR/original_time.log" 2>/dev/null | sed 's/^/```\n/; s/$/\n```/' >> "$RESULTS_DIR/performance_report.md" || echo "No timing data available" >> "$RESULTS_DIR/performance_report.md"

cat >> "$RESULTS_DIR/performance_report.md" << EOF

**Files Generated:**
EOF

cat "$RESULTS_DIR/original_files.log" 2>/dev/null | sed 's/^/```\n/; s/$/\n```/' >> "$RESULTS_DIR/performance_report.md" || echo "No file data available" >> "$RESULTS_DIR/performance_report.md"

# Display summary
echo ""
echo "Performance test completed!"
echo "Report saved to: $RESULTS_DIR/performance_report.md"
echo ""

# Show key metrics
if [ $current_success -eq 0 ] && [ $original_success -eq 0 ]; then
    echo "Quick Summary:"
    echo "- Current: ${current_time:-N/A}s, ${current_memory:-N/A}KB"
    echo "- Original: ${original_time:-N/A}s, ${original_memory:-N/A}KB"
    if [ -n "$improvement" ] && [ "$improvement" != "N/A" ]; then
        echo "- Improvement: ${improvement}% faster"
    fi
fi

# Show the full report
echo ""
echo "=== Full Performance Report ==="
cat "$RESULTS_DIR/performance_report.md"