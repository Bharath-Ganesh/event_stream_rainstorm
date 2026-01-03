#!/bin/bash

set -e

list_processes() {
    PIDS=$(pgrep -f "mp2_main" 2>/dev/null || true)
    if [ -z "$PIDS" ]; then
        echo "No MP2 processes found"
        return
    fi
    
    echo "PID    PORT    COMMAND"
    echo "---    ----    -------"
    for pid in $PIDS; do
        cmd=$(ps -p $pid -o command --no-headers 2>/dev/null || echo "Unknown")
        port=$(echo $cmd | grep -o '\-port=[0-9]*' | cut -d= -f2 || echo "Unknown")
        echo "$pid    $port    $cmd"
    done
}

kill_by_port() {
    local port=$1
    local pid=$(lsof -ti:$port 2>/dev/null)
    
    if [ -z "$pid" ]; then
        echo "No process on port $port"
        return 1
    fi
    
    kill -9 $pid 2>/dev/null || true
    echo "Killed process on port $port (PID: $pid)"
}

kill_by_pid() {
    local pid=$1
    if ! ps -p $pid >/dev/null 2>&1; then
        echo "Process $pid not found"
        return 1
    fi
    
    kill -9 $pid 2>/dev/null || true
    echo "Killed process $pid"
}

kill_all() {
    # Kill all MP2 processes using multiple methods for thoroughness
    pkill -f "mp2_main" 2>/dev/null || true
    pkill -f "go run.*-port" 2>/dev/null || true
    sleep 1
    echo "Killed all MP2 processes"
}

# Parse arguments
case "$1" in
    "list"|"l") list_processes ;;
    "port"|"p") [ -z "$2" ] && echo "Usage: $0 port <port_number>" && exit 1; kill_by_port "$2" ;;
    "pid"|"i") [ -z "$2" ] && echo "Usage: $0 pid <pid_number>" && exit 1; kill_by_pid "$2" ;;
    "all"|"a") kill_all ;;
    *) echo "Usage: $0 {list|port|pid|all} [argument]"; echo "  list - List all MP2 processes"; echo "  port - Kill process on specific port"; echo "  pid - Kill process by PID"; echo "  all - Kill all MP2 processes"; echo ""; echo "Examples:"; echo "  $0 list"; echo "  $0 port 8081"; echo "  $0 pid 12345"; echo "  $0 all" ;;
esac
