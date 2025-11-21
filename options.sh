#!/bin/bash

# options.sh - Test all command line switches for dsq
# This script runs dsq with each command line option to ensure they work correctly

passed=0
failed=0
failed_options=()

# Check if gum is installed
if ! command -v gum &> /dev/null; then
    echo "gum is not installed. Please install it from https://github.com/charmbracelet/gum"
    exit 1
fi

# Create log directory if it doesn't exist
mkdir -p log

# Test data file
TEST_DATA='{"name": "Alice", "age": 30, "city": "New York"}'

# Function to run a test
run_test() {
    local description="$1"
    local command="$2"

    echo "Testing: $description"
    echo "Command: $command"

    # Create temporary test data
    echo "$TEST_DATA" > /tmp/test_data.json

    # Run the command with timeout
    output=$(eval "timeout 30 $command" 2>&1)
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        echo "✓ PASSED: $description"
        ((passed++))
    else
        if [ $exit_code -eq 124 ]; then
            echo "✗ FAILED (timeout): $description" >&2
            echo "Exit code: $exit_code (timeout)" >&2
        else
            echo "✗ FAILED: $description" >&2
            echo "Exit code: $exit_code" >&2
        fi
        echo "Output: $output" >&2
        echo "FAILED: $description" >> log/options_testing.log
        echo "Command: $command" >> log/options_testing.log
        echo "Exit code: $exit_code" >> log/options_testing.log
        echo "Output: $output" >> log/options_testing.log
        echo "---" >> log/options_testing.log
        ((failed++))
        failed_options+=("$description")
    fi

    # Clean up
    rm -f /tmp/test_data.json /tmp/test_output.json /tmp/test_output.csv

    echo ""
}

echo "Starting dsq command line options testing..."
echo "Log file: log/options_testing.log"
echo ""

# Test basic functionality first
run_test "Basic filter execution" "cargo run --bin dsq -- '.' /tmp/test_data.json"

# Test output options
run_test "Compact output" "cargo run --bin dsq -- -c '.' /tmp/test_data.json"
run_test "Raw output" "cargo run --bin dsq -- -r '.' /tmp/test_data.json"
run_test "Sort keys" "cargo run --bin dsq -- -S '.' /tmp/test_data.json"
run_test "Tab indentation" "cargo run --bin dsq -- --tab '.' /tmp/test_data.json"
run_test "Custom indent" "cargo run --bin dsq -- --indent 4 '.' /tmp/test_data.json"
run_test "Color always" "cargo run --bin dsq -- -C always '.' /tmp/test_data.json"
run_test "Color never" "cargo run --bin dsq -- -C never '.' /tmp/test_data.json"
run_test "Join output" "cargo run --bin dsq -- -j '.' /tmp/test_data.json"

# Test input options
run_test "Slurp mode" "cargo run --bin dsq -- -s '.' /tmp/test_data.json"
run_test "Null input" "cargo run --bin dsq -- -n '.'"
run_test "Exit status" "cargo run --bin dsq -- -e '.age > 25' /tmp/test_data.json"

# Test format options
run_test "Input format JSON" "cargo run --bin dsq -- -i json '.' /tmp/test_data.json"
run_test "Output format JSON" "cargo run --bin dsq -- --output-format json '.' /tmp/test_data.json -o /tmp/test_output.json"

# Test CSV options
run_test "CSV separator" "cargo run --bin dsq -- --csv-separator ';' '.' /tmp/test_data.json"
run_test "CSV headers true" "cargo run --bin dsq -- --csv-headers true '.' /tmp/test_data.json"
run_test "CSV headers false" "cargo run --bin dsq -- --csv-headers false '.' /tmp/test_data.json"
run_test "CSV quote" "cargo run --bin dsq -- --csv-quote '\\\"' '.' /tmp/test_data.json"
run_test "CSV null values" "cargo run --bin dsq -- --csv-null 'NULL' '.' /tmp/test_data.json"

# Test processing options
run_test "Skip rows" "cargo run --bin dsq -- --skip-rows 1 '.' /tmp/test_data.json"
run_test "Limit rows" "cargo run --bin dsq -- --limit 1 '.' /tmp/test_data.json"
run_test "Select columns" "cargo run --bin dsq -- --select name,age '.' /tmp/test_data.json"
run_test "Lazy evaluation false" "cargo run --bin dsq -- --lazy false '.' /tmp/test_data.json"
run_test "DataFrame optimizations false" "cargo run --bin dsq -- --dataframe-optimizations false '.' /tmp/test_data.json"

# Test performance options
run_test "Batch size" "cargo run --bin dsq -- --batch-size 1000 '.' /tmp/test_data.json"
run_test "Memory limit" "cargo run --bin dsq -- --memory-limit 1GB '.' /tmp/test_data.json"
run_test "Threads" "cargo run --bin dsq -- --threads 2 '.' /tmp/test_data.json"
run_test "Parallel false" "cargo run --bin dsq -- --parallel false '.' /tmp/test_data.json"

# Test debug options
run_test "Explain" "cargo run --bin dsq -- --explain '.' /tmp/test_data.json"
run_test "Stats" "cargo run --bin dsq -- --stats '.' /tmp/test_data.json"
run_test "Time" "cargo run --bin dsq -- --time '.' /tmp/test_data.json"
run_test "Verbose" "cargo run --bin dsq -- -v '.' /tmp/test_data.json"
run_test "Quiet" "cargo run --bin dsq -- --quiet '.' /tmp/test_data.json"

# Test other options
run_test "Overwrite" "cargo run --bin dsq -- --overwrite '.' /tmp/test_data.json -o /tmp/test_output.json"
run_test "Test mode" "cargo run --bin dsq -- --test '.' /tmp/test_data.json"

# Test variable options (using simple values)
run_test "Arg variable" "cargo run --bin dsq -- --arg test_var value '.' /tmp/test_data.json"
run_test "Argjson variable" "cargo run --bin dsq -- --argjson test_var '{\"key\": \"value\"}' '.' /tmp/test_data.json"
run_test "Library path" "cargo run --bin dsq -- -L /tmp '.' /tmp/test_data.json"
run_test "Import" "cargo run --bin dsq -- --import test_module '.' /tmp/test_data.json"
run_test "Include" "cargo run --bin dsq -- --include /tmp/test_include.dsq '.' /tmp/test_data.json"

# Test subcommands
echo '[{"name": "Alice", "age": 30, "city": "New York"}]' > /tmp/test_convert_data.json
run_test "Convert subcommand" "cargo run --bin dsq -- convert /tmp/test_convert_data.json /tmp/test_output.csv --overwrite"
rm -f /tmp/test_convert_data.json
run_test "Inspect subcommand" "cargo run --bin dsq -- inspect /tmp/test_data.json"
run_test "Validate subcommand" "cargo run --bin dsq -- validate /tmp/test_data.json"
run_test "Completions subcommand" "cargo run --bin dsq -- completions bash"
run_test "Config show" "cargo run --bin dsq -- config show"
run_test "Config init" "cargo run --bin dsq -- config init /tmp/test_config.toml --force"

# Test filter file option
echo '.' > /tmp/test_filter.dsq
run_test "Filter file" "cargo run --bin dsq -- -f /tmp/test_filter.dsq /tmp/test_data.json"
rm -f /tmp/test_filter.dsq

# Test config file option
run_test "Config file" "cargo run --bin dsq -- --config /tmp/test_config.toml '.' /tmp/test_data.json"

echo "Summary: Passed: $passed, Failed: $failed"

if [ ${#failed_options[@]} -gt 0 ]; then
    echo "Failed options:"
    printf '%s\n' "${failed_options[@]}"
    printf '%s\n' "${failed_options[@]}" > log/failed_options.txt
    echo "Failed options written to log/failed_options.txt"
fi
