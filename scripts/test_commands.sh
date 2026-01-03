#!/bin/bash

# MP2 Test Commands Script - Based on run.sh functions
set -e

# Import functions from run.sh
source scripts/run.sh 2>/dev/null || true

# Test commands function
test_mp2_commands() {
    echo "Testing MP2 commands..."
    
    # Check if nodes are running
    if ! pgrep -f mp2_main > /dev/null; then
        echo "Error: No MP2 nodes are running. Start them first with: ./scripts/start_nodes.sh"
        exit 1
    fi
    
    echo "Nodes are running. Testing commands..."
    echo ""
    
    # Test introducer (port 8080)
    echo "=== Testing Introducer (localhost:8080) ==="
    
    echo "1. Testing list_self:"
    echo "list_self" | nc localhost 8080
    echo ""
    
    echo "2. Testing list_mem:"
    echo "list_mem" | nc localhost 8080
    echo ""
    
    echo "3. Testing display_protocol:"
    echo "display_protocol" | nc localhost 8080
    echo ""
    
    echo "4. Testing display_suspects:"
    echo "display_suspects" | nc localhost 8080
    echo ""
    
    echo "5. Testing grep (searching for 'INFO'):"
    echo "grep INFO" | nc localhost 8080
    echo ""
    
    # Test member nodes
    for port in 8081 8082 8083; do
        echo "=== Testing Member (localhost:$port) ==="
        
        echo "1. Testing list_self:"
        echo "list_self" | nc localhost $port
        echo ""
        
        echo "2. Testing list_mem:"
        echo "list_mem" | nc localhost $port
        echo ""
        
        echo "3. Testing display_protocol:"
        echo "display_protocol" | nc localhost $port
        echo ""
        
        echo "4. Testing display_suspects:"
        echo "display_suspects" | nc localhost $port
        echo ""
    done
    
    echo "All command tests completed"
}

# Run the function
test_mp2_commands