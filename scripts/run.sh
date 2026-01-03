#!/bin/bash

# Run Script for MP2
set -e

# Load environment variables
if [ -f "config.env" ]; then
    source config.env
fi

kill_all_daemons() {
    local scope=${1:-"local"}
    
    # Kill local processes
    pkill -f "mp2_main" || true
    pkill -f "go run.*-port" || true
    
    # Kill VM processes if requested
    if [ "$scope" = "vms" ] && [ -f "vm_deploy.sh" ]; then
        echo "Killing processes on all VMs..."
        source config.env
        ./vm_deploy.sh stop
    fi
    
    sleep 1
}

# Simple VM functions using config.json
start_introducer() {
    local port=${1:-"8080"}
    
    echo "Starting introducer on port $port..."
    go run mp2_main/mp2_main.go -port=$port -tcp=:$port &
    echo "Introducer started!"
}

mass_join() {
    local config_file="$1"
    shift 1  # Remove config_file from arguments
    
    # Check if second argument is a number (VM number) or a port
    # If it's a port (all digits), use it; otherwise use default
    local command_port=""
    if [ -n "$1" ] && [[ "$1" =~ ^[0-9]+$ ]] && [ "$1" -ge 1000 ] && [ "$1" -le 65535 ]; then
        # Looks like a port number (between 1000-65535)
        command_port="$1"
        shift 1  # Remove command_port from arguments
    else
        # Not a port, use default from env var or 9080
        command_port=${COMMAND_PORT:-9080}
    fi
    
    local ssh_key_path=${SSH_KEY_PATH:-"/Users/bganesh2/.ssh/id_ed25519"}
    local ssh_username=${SSH_USERNAME:-"bganesh2"}
    
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    
    for vm_num in "$@"; do
        local vm_name="VM$vm_num"
        local member=$(jq -r ".members[] | select(.name == \"$vm_name\")" "$config_file")
        local vm_address=$(echo "$member" | jq -r '.address')
        local port=$(echo "$vm_address" | cut -d: -f2)
        local host=$(echo "$vm_address" | cut -d: -f1)
        
        ssh -i "$ssh_key_path" "$ssh_username@$host" "cd mp4_g82 && go run mp4_main/mp4_main.go -port=$port -introducer=$introducer_address -commandport=$command_port" &
    done
}

mass_join_local() {
    local vm_count=${1:-4}
    local delay=${2:-1}
    local config_file=${3:-"config_local.json"}
    
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    
    echo "Local testing: Joining $vm_count VMs to introducer $introducer_address..."
    
    for i in $(seq 1 $vm_count); do
        local member=$(jq -r ".members[$((i-1))]" "$config_file")
        local vm_address=$(echo "$member" | jq -r '.address')
        local port=$(echo "$vm_address" | cut -d: -f2)
        
        echo "Starting VM$((i+1)) on localhost:$port..."
        go run mp2_main/mp2_main.go -port=$port -introducer=$introducer_address -tcp=:$port &
        
        if [ $delay -gt 0 ] && [ $i -lt $vm_count ]; then
            sleep $delay
        fi
    done
    
    echo "Local mass join completed!"
}

switch_old() {
    local protocol=${1:-"gossip"}
    local suspicion=${2:-"suspect"}
    local config_file=${3:-"config.json"}
    
    echo "Switching all VMs to protocol: $protocol, suspicion: $suspicion"
    
    # Switch introducer
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    echo "switch $protocol $suspicion" | nc $(echo "$introducer_address" | tr ':' ' ')
    
    # Switch all members (10 VMs total)
    for i in $(seq 1 9); do
        local member=$(jq -r ".members[$((i-1))]" "$config_file")
        local vm_address=$(echo "$member" | jq -r '.address')
        echo "switch $protocol $suspicion" | nc $(echo "$vm_address" | tr ':' ' ')
        sleep 0.5
    done
    
    echo "Switch completed!"
}

switch() {
    local protocol=${1:-"gossip"}
    local suspicion=${2:-"suspect"}
    local config_file=${3:-"config_4vms.json"}
    
    echo "Switching all VMs to: $protocol $suspicion (async)"
    
    # Switch introducer (async)
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    echo "switch $protocol $suspicion" | nc $(echo "$introducer_address" | tr ':' ' ') &
    
    # Switch all members (async)
    jq -r '.members[].address' "$config_file" | while read vm_address; do
        echo "switch $protocol $suspicion" | nc $(echo "$vm_address" | tr ':' ' ') &
    done
    
    echo "Switch commands sent to all VMs (async)!"
}

mass_crash() {
    local config_file="config_demo.json"
    local ssh_key_path=${SSH_KEY_PATH:-"/Users/bganesh2/.ssh/id_ed25519"}
    local ssh_username=${SSH_USERNAME:-"bganesh2"}
    
    for vm_num in "$@"; do
        local vm_name="VM$vm_num"
        local member=$(jq -r ".members[] | select(.name == \"$vm_name\")" "$config_file")
        local vm_address=$(echo "$member" | jq -r '.address')
        local host=$(echo "$vm_address" | cut -d: -f1)
        
        ssh -i "$ssh_key_path" "$ssh_username@$host" "cd mp4_g82 && pkill -f mp4_main" &
    done
}

join() {
    local vm_name=$1
    local config_file=$2
    
    # Set default SSH values if not loaded from config.env
    local ssh_key_path=${SSH_KEY_PATH:-"/Users/bganesh2/.ssh/id_ed25519"}
    local ssh_username=${SSH_USERNAME:-"bganesh2"}
    
    local member=$(jq -r ".members[] | select(.name == \"$vm_name\")" "$config_file")
    local vm_address=$(echo "$member" | jq -r '.address')
    local host=$(echo "$vm_address" | cut -d: -f1)
    local port=$(echo "$vm_address" | cut -d: -f2)
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    
    ssh -i "$ssh_key_path" "$ssh_username@$host" "cd mp4_g82 && go run mp4_main/mp4_main.go -port=$port -introducer=$introducer_address -tcp=:$port" &
}

join_local() {
    local vm_name=$1
    local config_file=$2
    
    local member=$(jq -r ".members[] | select(.name == \"$vm_name\")" "$config_file")
    local vm_address=$(echo "$member" | jq -r '.address')
    local port=$(echo "$vm_address" | cut -d: -f2)
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    
    go run mp2_main/mp2_main.go -port=$port -introducer=$introducer_address -tcp=:$port &
}

# Node management functions
start_nodes() {
    local config_file=${1:-"config.json"}
    
    if [ ! -f "$config_file" ]; then
        echo "Error: Config file '$config_file' not found"
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "Error: 'jq' is not installed. Please install it to parse the config file."
        exit 1
    fi
    
    kill_all_daemons
    
    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    local introducer_name=$(jq -r '.introducer.name' "$config_file")
    
    go run mp2_main/mp2_main.go -port=$(echo $introducer_address | cut -d: -f2) -tcp=:$(echo $introducer_address | cut -d: -f2) &
    sleep 3
    
    jq -r '.members[] | "\(.name)|\(.address)"' "$config_file" | while IFS='|' read -r name address; do
        local port=$(echo $address | cut -d: -f2)
        go run mp2_main/mp2_main.go -port=$port -introducer=$introducer_address -tcp=:$port &
        sleep 1
    done
}

test_commands() {
    local config_file=${1:-"config.json"}
    
    if [ ! -f "$config_file" ]; then
        echo "Error: Config file '$config_file' not found"
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        echo "Error: 'jq' is not installed. Please install it to parse the config file."
        exit 1
    fi
    
    if ! pgrep -f mp2_main > /dev/null; then
        echo "Error: No nodes are running. Start them first."
        exit 1
    fi

    local introducer_address=$(jq -r '.introducer.address' "$config_file")
    local host=$(echo $introducer_address | cut -d: -f1)
    local port=$(echo $introducer_address | cut -d: -f2)
    
    echo "list_self" | nc $host $port
    echo "list_mem" | nc $host $port
    echo "display_protocol" | nc $host $port
    echo "display_suspects" | nc $host $port
    echo "grep INFO" | nc $host $port
    
    jq -r '.members[] | "\(.name)|\(.address)"' "$config_file" | while IFS='|' read -r name address; do
        local host=$(echo $address | cut -d: -f1)
        local port=$(echo $address | cut -d: -f2)
        
        echo "list_self" | nc $host $port
        echo "list_mem" | nc $host $port
        echo "display_protocol" | nc $host $port
        echo "display_suspects" | nc $host $port
        echo "grep INFO" | nc $host $port
    done
}

if [ "$1" = "start" ]; then
    start_nodes "$2"
elif [ "$1" = "introducer" ]; then
    start_introducer "$2"
elif [ "$1" = "mass_join" ]; then
    shift 1
    # mass_join config_file command_port vm_numbers...
    # If command_port is not provided, it will use default from COMMAND_PORT env var or 9080
    mass_join "$@"
elif [ "$1" = "mass_join_local" ]; then
    mass_join_local "$2" "$3" "$4"
elif [ "$1" = "switch" ]; then
    switch "$2" "$3" "$4"
elif [ "$1" = "mass_crash" ]; then
    shift 1
    mass_crash "$@"
elif [ "$1" = "join" ]; then
    join "$2" "$3"
elif [ "$1" = "join_local" ]; then
    join_local "$2" "$3"
elif [ "$1" = "stop" ]; then
    kill_all_daemons "local"
elif [ "$1" = "stop_vms" ]; then
    kill_all_daemons "vms"
elif [ "$1" = "test" ]; then
    test_commands "$2"
elif [ "$1" = "setup" ]; then
    kill_all_daemons "local"
    sleep 2
    start_nodes "$2"
    sleep 5
    test_commands "$2"
else
    echo "Usage: $0 {start|introducer|mass_join|mass_join_local|switch|mass_crash|join|stop|stop_vms|test|setup} [args...]"
    echo ""
    echo "Examples:"
    echo "  $0 introducer 8080                       - Start introducer on port 8080"
    echo "  $0 mass_join config.json 6080 2 3 4     - Join VMs 2,3,4 with command port 6080"
    echo "  $0 mass_join config.json 2 3 4           - Join VMs 2,3,4 (uses COMMAND_PORT env var or default 9080)"
    echo "  $0 mass_join_local 3 1 config_local.json - Join 3 VMs locally for testing"
    echo "  $0 switch ping nosuspect                 - Switch all VMs to ping+nosuspect (async)"
    echo "  $0 switch gossip suspect config.json     - Switch all VMs to gossip+suspect"
    echo "  $0 mass_crash config.json 1 2 3          - Crash VMs 2,3,4 simultaneously"
    echo "  $0 join VM2 config.json                  - Join individual VM2"
fi
