#!/bin/bash

set -e

# Get actual hostname and IP
current_hostname=$(hostname)
current_ip=$(hostname -I | awk '{print $1}')

echo "PID    PORT  IP        PROTOCOLS  TYPE"
echo "---    ----  --        ---------  ----"
for pid in $(pgrep -f "mp2_main"); do
    cmd=$(ps -p $pid -o command 2>/dev/null | tail -n +2)
    port=$(echo $cmd | grep -o '\-port=[0-9]*' | cut -d= -f2)
    
    if [ -z "$port" ]; then
        continue
    fi
    
    if echo $cmd | grep -q "introducer="; then
        node_type="Member"
    else
        node_type="Introducer"
    fi
    
    # Check if it's the main go process or the compiled binary
    if echo $cmd | grep -q "go run"; then
        protocols="UDP"
    else
        protocols="TCP"
    fi
    
    echo "$pid $port $current_ip $protocols $node_type"
done