#!/bin/bash

# MP2 Start Nodes Script - Based on run.sh functions
set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Import functions from run.sh
source "$SCRIPT_DIR/run.sh"

# Check if config file is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <config_file>"
    echo "Example: $0 config_local.json"
    exit 1
fi

CONFIG_FILE="$1"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file '$CONFIG_FILE' not found"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' is not installed. Please install it to parse the config file."
    echo "On macOS: brew install jq"
    exit 1
fi

echo "Starting MP2 nodes with config: $CONFIG_FILE"
start_nodes "$CONFIG_FILE"