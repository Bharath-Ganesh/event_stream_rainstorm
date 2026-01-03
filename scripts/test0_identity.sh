#!/bin/bash
# Test 0: Application 0 - Identity Operator
# Pre-test for basic RainStorm functionality

set -e

echo "=========================================="
echo "Test 0: Application 0 - Identity Operator"
echo "=========================================="
echo ""

# Configuration
INPUT_FILE="dataset1.csv"
OUTPUT_FILE="output_app0"
NUM_STAGES=1
NUM_TASKS=3
OPERATOR="op0_identity"
OP_ARGS=""
EXACTLY_ONCE=true
AUTOSCALE=false
INPUT_RATE=100

echo "Configuration:"
echo "  Input File: $INPUT_FILE"
echo "  Output File: $OUTPUT_FILE"
echo "  Stages: $NUM_STAGES"
echo "  Tasks per Stage: $NUM_TASKS"
echo "  Operator: $OPERATOR"
echo "  Exactly-Once: $EXACTLY_ONCE"
echo "  Autoscale: $AUTOSCALE"
echo "  Input Rate: $INPUT_RATE tuples/sec"
echo ""

# Step 1: Build operator
echo "Step 1: Building operator..."
./vm_deploy.sh build $OPERATOR ./ops/identity/op0_identity.go
echo "✓ Operator built successfully"
echo ""

# Step 2: Create input file on HyDFS (if not already exists)
echo "Step 2: Creating input file on HyDFS..."
if [ -f "$INPUT_FILE" ]; then
    go run ./demo create $INPUT_FILE $INPUT_FILE || echo "File may already exist"
    echo "✓ Input file created on HyDFS"
else
    echo "✗ Error: $INPUT_FILE not found locally"
    exit 1
fi
echo ""

# Step 3: Start RainStorm job
echo "Step 3: Starting RainStorm job..."
echo "Command: go run ./demo RainStorm $NUM_STAGES $NUM_TASKS $OPERATOR \"$OP_ARGS\" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE"
echo ""
go run ./demo RainStorm $NUM_STAGES $NUM_TASKS $OPERATOR "$OP_ARGS" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE
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

# Step 5: Verify correctness
echo "Step 5: Verifying correctness..."
echo ""

# Line count comparison
ORIGINAL_LINES=$(wc -l < $INPUT_FILE | tr -d ' ')
OUTPUT_LINES=$(wc -l < local_$OUTPUT_FILE.txt | tr -d ' ')

echo "Original file lines: $ORIGINAL_LINES"
echo "Output file lines: $OUTPUT_LINES"

if [ "$ORIGINAL_LINES" -eq "$OUTPUT_LINES" ]; then
    echo "✓ Line count matches!"
else
    echo "✗ Line count mismatch!"
    exit 1
fi
echo ""

# Content comparison (sorted)
echo "Comparing content (sorted)..."
sort $INPUT_FILE > sorted_original.csv
sort local_$OUTPUT_FILE.txt > sorted_output.csv

if diff sorted_original.csv sorted_output.csv > /dev/null; then
    echo "✓ Content matches! Test 0 PASSED."
else
    echo "✗ Content mismatch! Showing differences:"
    diff sorted_original.csv sorted_output.csv | head -20
    exit 1
fi
echo ""

# Clean up temp files
rm sorted_original.csv sorted_output.csv

echo "=========================================="
echo "Test 0 Complete - SUCCESS"
echo "=========================================="
echo ""
echo "Output file saved as: local_$OUTPUT_FILE.txt"
echo ""
echo "To monitor logs during the test:"
echo "  Leader: tail -f slog/rs_leader_*.log"
echo "  Tasks: tail -f slog/rs_Stage1_Task0_*.log"

