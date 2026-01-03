#!/bin/bash
set -em

# Load environment variables
if [ -f "config.env" ]; then
    source config.env
else
    echo "Error: config.env file not found. Please create it with the required environment variables."
    exit 1
fi

# Clone repository to all VMs
git_clone() {
  echo "Cloning MP4 repository to all VMs..."
  for i in $(seq 1 $SERVER_COUNT)
  do
    echo "Cloning on VM $i..."
    ssh $SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu "git clone https://$GITLAB_USERNAME:$GITLAB_TOKEN@$GITLAB_URL/$GITLAB_REPO.git; exit"
  done
}

# Pull latest changes from specific branch
git_pull() {
  branch=${1:-main}  
  echo "Pulling latest changes from branch '$branch' on all VMs..."
  for i in $(seq 1 $SERVER_COUNT)
  do
    echo "Pulling on VM $i..."
    ssh -i "$SSH_KEY_PATH" "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" "cd mp4_g82 && rm -f config.env && git fetch origin && git clean -fd && git checkout -B $branch origin/$branch"
  done
}

stop_all() {
  echo "Stopping all MP4 processes..."
  for i in $(seq 1 $SERVER_COUNT)
  do
    echo "Stopping processes on VM $i..."
    ssh -i "$SSH_KEY_PATH" "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" "pkill -f mp4_main 2>/dev/null; pkill -f 'go run.*mp4_main' 2>/dev/null" || true
  done
}

# Clear application log files on all VMs
clear_logs() {
  echo "Clearing application log files on all VMs..."
  for i in $(seq 1 10)
  do
    echo "Clearing logs on VM $i..."
    ssh -i "$SSH_KEY_PATH" "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" "cd mp2_g82 && rm -rf logfiles/*.log 2>/dev/null && rm -rf *.log 2>/dev/null" 2>/dev/null || echo "VM $i not accessible"
  done
  echo "Log clearing completed!"
}

# Build Go executable on all VMs
go_build() {
  exe_name=$1
  source_file=$2
  if [ -z "$exe_name" ] || [ -z "$source_file" ]; then
    echo "Error: Missing arguments."
    echo "Usage: ./script.sh build <executable_name> <source_file>"
    exit 1
  fi

  echo "Building '$source_file' to '$exe_name' on all VMs..."
  
  for i in $(seq 1 $SERVER_COUNT)
  do
    echo "Building on VM $i..."
    ssh -i "$SSH_KEY_PATH" "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" \
      "cd mp4_g82 && go build -o $exe_name $source_file"
  done
  echo "Build finished on all VMs."
}

# Push local big file(dataset1.csv) to remote directory on all VMs
push_file() {
  local_file=$1
  remote_dir="mp4_g82"

  if [ -z "$local_file" ]; then
    echo "Usage: $0 push_file <local_file_path>"
    exit 1
  fi

  if [ ! -f "$local_file" ]; then
    echo "Error: Local file '$local_file' does not exist."
    exit 1
  fi

  filename=$(basename "$local_file")
  echo "Pushing local file '$local_file' to remote directory '$remote_dir' on all VMs..."

  for i in $(seq 1 $SERVER_COUNT)
  do
    vm_num=$(printf "%02d" $i)
    host="$SSH_USERNAME@$SERVER_PREFIX$vm_num.cs.illinois.edu"
    echo "Uploading to VM $i..."
    
    scp -i "$SSH_KEY_PATH" "$local_file" "$host:$remote_dir/$filename"
  done
  echo "File push completed!"
}

# Clean OpExe only on all VMs
clean_op_tasks() {
  echo "Cleaning up RainStorm Operator processes on all VMs..."
  set +e  # Don't exit on error

  for i in $(seq 1 $SERVER_COUNT); do
    echo "Connecting to VM $i..."

    ssh -o StrictHostKeyChecking=no \
        -i "$SSH_KEY_PATH" \
        "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" \
        "
        echo '[VM $i] Checking operators...';

        OPS=\$(pgrep -fl 'op[0-9]_');

        if [ -z \"\$OPS\" ]; then
            echo '[VM $i] No operators running.'
        else
            echo '[VM $i] Killing the following operators:'
            echo \"\$OPS\"
            pkill -9 -f 'op[0-9]_' || true
            echo '[VM $i] Kill complete.'
        fi
        "
  done

  set -e
  echo "All operator cleanup completed."
}

clean_home() {
  echo "Cleaning home directory on all VMs (keeping only mp4_g82)..."
  for i in $(seq 1 $SERVER_COUNT)
  do
    echo "Cleaning on VM $i..."
    ssh -i "$SSH_KEY_PATH" "$SSH_USERNAME@$SERVER_PREFIX$(printf "%02d" $i).cs.illinois.edu" << 'ENDSSH'
      cd ~
      for item in *; do
        if [ "$item" != "mp4_g82" ] && [ -e "$item" ]; then
          rm -rf "$item" 2>/dev/null || true
        fi
      done
      for item in .*; do
        if [ "$item" != "." ] && [ "$item" != ".." ] && [ "$item" != ".bashrc" ] && [ "$item" != ".bash_logout" ] && [ "$item" != ".profile" ] && [ "$item" != ".ssh" ] && [ "$item" != "mp4_g82" ]; then
          rm -rf "$item" 2>/dev/null || true
        fi
      done
ENDSSH
  done
  echo "Cleanup complete!"
}

# Copy file from a VM to local directory
copy_from() {
  local vm_num=$1
  local remote_file=$2
  local dest_dir=${3:-"."}

  if [ -z "$vm_num" ] || [ -z "$remote_file" ]; then
    echo "Usage: $0 copy_from <vm_num> <remote_file_path> [destination_dir]"
    echo "Example: $0 copy_from 1 ~/mp4_g82/local_dataset1.csv"
    echo "Example: $0 copy_from 1 ~/mp4_g82/local_dataset1.csv 'test results'"
    exit 1
  fi

  # Validate VM number
  if ! [[ "$vm_num" =~ ^[0-9]+$ ]] || [ "$vm_num" -lt 1 ] || [ "$vm_num" -gt "$SERVER_COUNT" ]; then
    echo "Error: Invalid VM number. Must be between 1 and $SERVER_COUNT"
    exit 1
  fi

  # Create destination directory if it doesn't exist
  mkdir -p "$dest_dir"

  # VM hostname
  local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $vm_num).cs.illinois.edu"

  # Extract filename for local destination
  local filename=$(basename "$remote_file")

  # Copy file from VM to local
  # If path was expanded locally (starts with /Users/ or /home/), reconstruct with ~
  # Otherwise use as-is (scp will expand ~ on remote side)
  local scp_remote_path="$remote_file"
  if [[ "$remote_file" =~ ^/(Users|home)/ ]]; then
    # Path was expanded locally, reconstruct with ~/mp4_g82/ if it contains mp4_g82
    if [[ "$remote_file" == *"/mp4_g82/"* ]]; then
      scp_remote_path="~/mp4_g82/${remote_file##*/mp4_g82/}"
    fi
  fi

  echo "Copying $remote_file from VM$vm_num to $dest_dir..."
  scp -i "$SSH_KEY_PATH" "${vm_host}:${scp_remote_path}" "$dest_dir/$filename"

  if [ $? -eq 0 ]; then
    echo "Successfully copied $remote_file from VM$vm_num to $dest_dir/$filename"
  else
    echo "Error: Failed to copy $remote_file from VM$vm_num"
    exit 1
  fi
}

if [ "$1" = "clone" ]; then
  git_clone
elif [ "$1" = "pull" ]; then
  git_pull "$2"
elif [ "$1" = "stop" ]; then
  stop_all
elif [ "$1" = "clear_logs" ]; then
  clear_logs
elif [ "$1" = "build" ]; then
  go_build "$2" "$3"
elif [ "$1" = "push_file" ]; then
  push_file "$2"
elif [ "$1" = "clean_op_tasks" ]; then
  clean_op_tasks
elif [ "$1" = "clean_home" ]; then
  clean_home
elif [ "$1" = "copy_from" ]; then
  copy_from "$2" "$3" "$4"
else
  echo "Usage: $0 {clone|pull|stop|clear_logs|build|push_file|clean_op_tasks|clean_home|copy_from}"
  echo "Build Example: $0 build op0_identity op0_identity.go"
  echo "  $0 push_file dataset1.csv"
  echo "  $0 copy_from <vm_num> <remote_file_path> [destination_dir]"
  echo "  $0 copy_from 1 ~/mp4_g82/local_dataset1.csv"
  echo "  $0 clean_home"
fi