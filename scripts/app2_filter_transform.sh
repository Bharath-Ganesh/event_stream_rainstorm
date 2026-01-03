#!/bin/bash
# Application 2: Filter & Transform
# Filter by pattern, then extract first 3 CSV fields

set -e

echo "=========================================="
echo "Application 2: Filter & Transform"
echo "=========================================="
echo ""

# Configuration - EDIT THESE FOR DEMO
PATTERN="Traffic"      # Pattern to filter (CHANGE DURING DEMO)
INPUT_FILE="dataset1.csv"
OUTPUT_FILE="output_app2"
NUM_STAGES=2
NUM_TASKS=3
EXACTLY_ONCE=true
AUTOSCALE=false
INPUT_RATE=100

echo "Configuration:"
echo "  Pattern: \"$PATTERN\""
echo "  Transform: Extract first 3 CSV fields"
echo "  Input File: $INPUT_FILE"
echo "  Output File: $OUTPUT_FILE"
echo "  Stages: $NUM_STAGES"
echo "  Tasks per Stage: $NUM_TASKS"
echo "  Exactly-Once: $EXACTLY_ONCE"
echo "  Autoscale: $AUTOSCALE"
echo "  Input Rate: $INPUT_RATE tuples/sec"
echo ""

# Step 1: Build operators
echo "Step 1: Building operators..."
./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go
./vm_deploy.sh build op3_transform ./ops/transform/op3_transform.go
echo "✓ Operators built successfully"
echo ""

# Step 2: Create input file on HyDFS (if not already exists)
echo "Step 2: Creating input file on HyDFS..."
if [ -f "$INPUT_FILE" ]; then
    go run ./demo create $INPUT_FILE $INPUT_FILE 2>/dev/null || echo "File may already exist"
    echo "✓ Input file created on HyDFS"
else
    echo "✗ Error: $INPUT_FILE not found locally"
    exit 1
fi
echo ""

# Step 3: Start RainStorm job
echo "Step 3: Starting RainStorm job..."
echo "Command: go run ./demo RainStorm $NUM_STAGES $NUM_TASKS op1_filter \"$PATTERN\" op3_transform \"\" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE"
echo ""
go run ./demo RainStorm $NUM_STAGES $NUM_TASKS op1_filter "$PATTERN" op3_transform "" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE
echo ""

# Wait for job to complete
echo "Waiting for job to complete..."
sleep 35

# Step 4: Retrieve output
echo ""
echo "Step 4: Retrieving output from HyDFS..."
go run ./demo get $OUTPUT_FILE local_$OUTPUT_FILE.txt
echo "✓ Output retrieved to local_$OUTPUT_FILE.txt"
echo ""

# Step 5: Verify results
echo "=========================================="
echo "Verification"
echo "=========================================="
echo ""

# Line count
OUTPUT_LINES=$(wc -l < local_$OUTPUT_FILE.txt | tr -d ' ')
echo "Total output lines: $OUTPUT_LINES"
echo ""

# Expected: count lines in input that contain pattern
EXPECTED_LINES=$(grep -c "$PATTERN" $INPUT_FILE || echo "0")
echo "Expected lines (grep \"$PATTERN\" $INPUT_FILE): $EXPECTED_LINES"
echo ""

# Show sample results
echo "Sample output (first 10 lines):"
head -10 local_$OUTPUT_FILE.txt
echo ""

echo "Sample output (last 10 lines):"
tail -10 local_$OUTPUT_FILE.txt
echo ""

# Verify format: each line should have exactly 3 CSV fields
echo "Verifying format (each line should have 3 fields)..."
FIELD_CHECK=$(awk -F',' '{if(NF!=3) print NR": "$0}' local_$OUTPUT_FILE.txt | head -5)
if [ -z "$FIELD_CHECK" ]; then
    echo "✓ All lines have exactly 3 fields"
else
    echo "⚠ Some lines have incorrect field count:"
    echo "$FIELD_CHECK"
fi
echo ""

# Compare with manual verification
echo "Manual verification:"
echo "  1. Input lines containing \"$PATTERN\":"
grep "$PATTERN" $INPUT_FILE | head -3
echo ""
echo "  2. Expected output (first 3 fields of above):"
grep "$PATTERN" $INPUT_FILE | head -3 | cut -d',' -f1-3
echo ""
echo "  3. Actual output (first 3 lines):"
head -3 local_$OUTPUT_FILE.txt
echo ""

# Summary
echo "=========================================="
echo "Application 2 Complete"
echo "=========================================="
echo ""
echo "Results:"
echo "  ✓ Job completed successfully"
echo "  ✓ Output file: local_$OUTPUT_FILE.txt"
echo "  ✓ Output lines: $OUTPUT_LINES"
echo "  ✓ Expected lines: $EXPECTED_LINES"
echo ""
echo "For detailed verification:"
echo "  cat local_$OUTPUT_FILE.txt"
echo "  # Compare with:"
echo "  grep \"$PATTERN\" $INPUT_FILE | cut -d',' -f1-3"
echo ""

