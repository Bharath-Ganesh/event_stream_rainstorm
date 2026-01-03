#!/bin/bash
# Test 1 with Autoscale Enabled
# Demonstrates dynamic scaling based on load

set -e

echo "=========================================="
echo "Test 1: Application 1 with Autoscale"
echo "=========================================="
echo ""

# Configuration - EDIT THESE FOR DEMO
PATTERN="Parking"      # Pattern to filter
GROUP_COLUMN=8         # Column to group by
INPUT_FILE="dataset1.csv"
OUTPUT_FILE="output_test1_autoscale"
NUM_STAGES=2
NUM_TASKS=1            # Start with 1 task per stage
EXACTLY_ONCE=true
AUTOSCALE=true
INPUT_RATE=100         # Input rate (tuples/sec)
LOW_WATERMARK=50       # Scale down if avg rate < 50
HIGH_WATERMARK=200     # Scale up if avg rate > 200

echo "Configuration:"
echo "  Pattern: \"$PATTERN\""
echo "  Group Column: $GROUP_COLUMN"
echo "  Input File: $INPUT_FILE"
echo "  Output File: $OUTPUT_FILE"
echo "  Stages: $NUM_STAGES"
echo "  Initial Tasks per Stage: $NUM_TASKS"
echo "  Exactly-Once: $EXACTLY_ONCE"
echo "  Autoscale: $AUTOSCALE"
echo "  Input Rate: $INPUT_RATE tuples/sec"
echo "  Low Watermark: $LOW_WATERMARK tuples/sec"
echo "  High Watermark: $HIGH_WATERMARK tuples/sec"
echo ""

# Step 1: Build operators
echo "Step 1: Building operators..."
./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go
./vm_deploy.sh build op2_count ./ops/count/op2_count.go
echo "✓ Operators built successfully"
echo ""

# Step 2: Create input file on HyDFS
echo "Step 2: Creating input file on HyDFS..."
if [ -f "$INPUT_FILE" ]; then
    go run ./demo create $INPUT_FILE $INPUT_FILE 2>/dev/null || echo "File may already exist"
    echo "✓ Input file created on HyDFS"
else
    echo "✗ Error: $INPUT_FILE not found locally"
    exit 1
fi
echo ""

# Step 3: Start RainStorm job with autoscale
echo "Step 3: Starting RainStorm job with autoscale..."
echo "Command: go run ./demo RainStorm $NUM_STAGES $NUM_TASKS op1_filter \"$PATTERN $GROUP_COLUMN\" op2_count \"$GROUP_COLUMN\" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE $LOW_WATERMARK $HIGH_WATERMARK"
echo ""
go run ./demo RainStorm $NUM_STAGES $NUM_TASKS op1_filter "$PATTERN $GROUP_COLUMN" op2_count "$GROUP_COLUMN" $INPUT_FILE $OUTPUT_FILE $EXACTLY_ONCE $AUTOSCALE $INPUT_RATE $LOW_WATERMARK $HIGH_WATERMARK
echo ""

echo "=========================================="
echo "Job Started - Monitoring Autoscale Events"
echo "=========================================="
echo ""
echo "What to watch for:"
echo "  1. Initial task deployment (1 task per stage)"
echo "  2. Rate monitoring (every 1 second)"
echo "  3. Scale up events (if avg rate > $HIGH_WATERMARK)"
echo "  4. Scale down events (if avg rate < $LOW_WATERMARK)"
echo ""
echo "Monitoring commands:"
echo ""
echo "  List current tasks:"
echo "    go run ./demo list_tasks"
echo ""
echo "  Monitor leader log for autoscale events (on VM1):"
echo "    tail -f slog/rs_leader_*.log | grep -E \"Autoscale|Task Started|Task Completed\""
echo ""
echo "  Monitor task rates (on VM1):"
echo "    tail -f slog/rs_leader_*.log | grep \"RATE\""
echo ""
echo "  Monitor specific task (after listing tasks):"
echo "    # SSH to task VM, then:"
echo "    tail -f slog/rs_Stage*_Task*_*.log"
echo ""

# Wait for job to complete
echo "Waiting for job to complete..."
sleep 35

# Retrieve output
echo ""
echo "Step 4: Retrieving output..."
go run ./demo get $OUTPUT_FILE local_$OUTPUT_FILE.txt
echo "✓ Output retrieved to local_$OUTPUT_FILE.txt"
echo ""

# Summary
echo "=========================================="
echo "Autoscale Test Complete"
echo "=========================================="
echo ""
OUTPUT_LINES=$(wc -l < local_$OUTPUT_FILE.txt | tr -d ' ')
echo "Results:"
echo "  ✓ Job completed successfully"
echo "  ✓ Output lines: $OUTPUT_LINES"
echo "  ✓ Output file: local_$OUTPUT_FILE.txt"
echo ""
echo "To verify autoscale behavior:"
echo "  1. Check leader log for autoscale events:"
echo "     grep -E \"Autoscale|Scale UP|Scale DOWN\" slog/rs_leader_*.log"
echo ""
echo "  2. Check rate logs to see load patterns:"
echo "     grep \"RATE\" slog/rs_leader_*.log | tail -20"
echo ""
echo "  3. Verify task lifecycle (start/stop during scaling):"
echo "     grep -E \"Task Started|Task Completed|Scaling\" slog/rs_leader_*.log"
echo ""

