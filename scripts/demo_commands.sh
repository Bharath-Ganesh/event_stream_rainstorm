#!/bin/bash
# Quick Reference: Demo Commands
# Use this as a cheatsheet during the demo

echo "=========================================="
echo "RainStorm Demo - Quick Command Reference"
echo "=========================================="
echo ""

echo "=== Setup Commands ==="
echo ""
echo "1. Start Leader (on VM1):"
echo "   go run ./mp4_main -port 9190 -commandport 9180"
echo ""

echo "2. Join Workers (from local, mass join VM2-10):"
echo "   ./scripts/run.sh mass_join config_demo.json 9180 2 3 4 5 6 7 8 9 10"
echo ""

echo "3. Build Operators (from local, deploy to all VMs):"
echo "   ./vm_deploy.sh build op0_identity ./ops/identity/op0_identity.go"
echo "   ./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go"
echo "   ./vm_deploy.sh build op2_count ./ops/count/op2_count.go"
echo "   ./vm_deploy.sh build op3_transform ./ops/transform/op3_transform.go"
echo ""

echo "=== Test Scripts ==="
echo ""
echo "Test 0 (Pre-test - Identity):"
echo "   ./scripts/test0_identity.sh"
echo ""

echo "Test 1 (Filter & Count - No Autoscale):"
echo "   ./scripts/test1_filter_count.sh"
echo "   # Edit PATTERN and GROUP_COLUMN in script before running"
echo ""

echo "Test 1 (Filter & Count - With Autoscale):"
echo "   ./scripts/test1_autoscale.sh"
echo ""

echo "Application 2 (Filter & Transform):"
echo "   ./scripts/app2_filter_transform.sh"
echo "   # Edit PATTERN in script before running"
echo ""

echo "=== Manual RainStorm Commands ==="
echo ""
echo "Application 0 (Identity):"
echo "   go run ./demo RainStorm 1 3 op0_identity \"\" dataset1.csv output_app0 true false 100"
echo ""

echo "Application 1 (Filter & Count):"
echo "   # Example with pattern=\"Parking\" and column=8"
echo "   go run ./demo RainStorm 2 3 op1_filter \"Parking 8\" op2_count \"8\" dataset1.csv output_app1 true false 100"
echo ""

echo "Application 1 with Autoscale:"
echo "   # LW=50, HW=200"
echo "   go run ./demo RainStorm 2 3 op1_filter \"Parking 8\" op2_count \"8\" dataset1.csv output_app1 true true 100 50 200"
echo ""

echo "Application 2 (Filter & Transform):"
echo "   # Example with pattern=\"Traffic\""
echo "   go run ./demo RainStorm 2 3 op1_filter \"Traffic\" op3_transform \"\" dataset1.csv output_app2 true false 100"
echo ""

echo "=== Monitoring Commands ==="
echo ""
echo "List running tasks:"
echo "   go run ./demo list_tasks"
echo ""

echo "Kill a task (example: VM5, PID 12345):"
echo "   go run ./demo kill_task VM5 12345"
echo ""

echo "Get output file from HyDFS:"
echo "   go run ./demo get output_app1 local_output_app1.txt"
echo ""

echo "List file replicas:"
echo "   go run ./demo ls output_app1"
echo ""

echo "Monitor leader log (on VM1):"
echo "   tail -f slog/rs_leader_*.log"
echo ""

echo "Monitor task log (on task VM):"
echo "   tail -f slog/rs_Stage*_Task*_*.log"
echo ""

echo "Monitor HyDFS output (on replica VM):"
echo "   tail -f hydfs/output_app1"
echo ""

echo "=== Verification Commands ==="
echo ""
echo "Count output lines:"
echo "   wc -l local_output_app1.txt"
echo ""

echo "Search output for keyword (example: 'NE'):"
echo "   grep 'NE' local_output_app1.txt | tail -3"
echo ""

echo "Verify timing (on VM1):"
echo "   grep -E 'Source Started|End of Entire Run' slog/rs_leader_*.log | tail -2"
echo ""

echo "Check autoscale events (on VM1):"
echo "   grep -E 'Autoscale|Scale UP|Scale DOWN' slog/rs_leader_*.log"
echo ""

echo "Check task rates (on VM1):"
echo "   grep 'RATE' slog/rs_leader_*.log | tail -20"
echo ""

echo "Check duplicates in task log:"
echo "   grep 'DUPLICATE' slog/rs_Stage*_Task*_*.log"
echo ""

echo "=== Crash/Recovery Commands ==="
echo ""
echo "Mass crash VMs (example: crash VM2-5):"
echo "   ./scripts/run.sh mass_crash config_demo.json 2 3 4 5"
echo ""

echo "Restart crashed VMs:"
echo "   ./scripts/run.sh mass_join config_demo.json 9180 2 3 4 5"
echo ""

echo "=== Cleanup Commands ==="
echo ""
echo "Kill all processes on all VMs:"
echo "   ./scripts/run.sh mass_crash config_demo.json 1 2 3 4 5 6 7 8 9 10"
echo ""

echo "Remove local test outputs:"
echo "   rm -f local_*.txt local_*.csv sorted_*.csv"
echo ""

echo "=========================================="
echo "Notes:"
echo "  - All commands assume you're in mp4_g82/ directory"
echo "  - Scripts have configurable parameters at the top"
echo "  - Monitor logs in real-time during tests"
echo "  - Check both leader and task logs for complete picture"
echo "=========================================="

