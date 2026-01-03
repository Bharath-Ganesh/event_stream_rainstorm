#!/bin/bash
# Apache Spark Cluster Management Script
# Manages Spark cluster installation, startup, and job submission
# - Master node (leader): VM1 on port 7077
# - Worker nodes: VMs 2-N connected to master
# - Jobs are submitted via spark-submit to the master node

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$PROJECT_ROOT/config.env" ]; then
    source "$PROJECT_ROOT/config.env"
else
    echo "Error: config.env file not found at $PROJECT_ROOT/config.env"
    exit 1
fi

SPARK_TARBALL=$(find "$PROJECT_ROOT" -maxdepth 1 -name "spark-*.tgz" | head -1)
if [ -z "$SPARK_TARBALL" ]; then
    echo "Error: Spark tarball not found in project root."
    exit 1
fi

SPARK_TARBALL_NAME=$(basename "$SPARK_TARBALL")
SPARK_DIR="${SPARK_TARBALL_NAME%.tgz}"

install_spark_on_vm() {
    local vm_num=$1
    local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $vm_num).cs.illinois.edu"
    
    ssh -T -i "$SSH_KEY_PATH" "$vm_host" << ENDSSH
        set -e
        
        cd ~/spark
        tar -xzf "$SPARK_TARBALL_NAME" --strip-components=1
        rm "$SPARK_TARBALL_NAME"
        
        if [ ! -f ~/spark/conf/spark-env.sh ]; then
            cp ~/spark/conf/spark-env.sh.template ~/spark/conf/spark-env.sh
            JAVA_HOME_VAL=\$(readlink -f /usr/bin/java | sed 's:bin/java::')
            echo "export JAVA_HOME=\$JAVA_HOME_VAL" >> ~/spark/conf/spark-env.sh
        fi
ENDSSH
    
    echo "[VM $vm_num] Installation complete"
}

start_master() {
    local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}01.cs.illinois.edu"
    echo "[VM1] Starting Apache Spark Master (leader node)..."
    ssh -T -i "$SSH_KEY_PATH" "$vm_host" "~/spark/sbin/stop-master.sh 2>/dev/null || true; sleep 1; ~/spark/sbin/start-master.sh"
    echo "[VM1] Apache Spark Master started at spark://${SERVER_PREFIX}01.cs.illinois.edu:7077"
}

start_workers() {
    # Start Apache Spark workers on all VMs (except VM1 which is the master)
    # Each worker connects to the master URL to join the cluster
    local master_url="spark://${SERVER_PREFIX}01.cs.illinois.edu:7077"
    for i in $(seq 2 $SERVER_COUNT); do
        local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
        ssh -T -i "$SSH_KEY_PATH" "$vm_host" "~/spark/sbin/stop-worker.sh 2>/dev/null || true; sleep 1; ~/spark/sbin/start-worker.sh $master_url" &
    done
    wait
    echo "Apache Spark Workers started and connected to master"
}

run_job() {
    local master_url="spark://${SERVER_PREFIX}01.cs.illinois.edu:7077"
    local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}01.cs.illinois.edu"
    
    if [ $# -lt 4 ]; then
        echo "Usage: $0 run <Nstages> <Ntasks> <op1> <op1_args> [op2] [op2_args] <input_file> [output_file]"
        echo "       Submit Apache Spark job to the cluster (master node executes the job)"
        echo "Example: $0 run 2 3 filter 'Parking 8' count '8' dataset1.csv spark_output"
        exit 1
    fi
    
    local num_stages=$1
    local num_tasks=$2
    shift 2
    
    echo "Running Apache Spark job: Stages=$num_stages, Tasks=$num_tasks"
    
    # Build Apache Spark submit command with proper quoting
    # This submits the job to the Spark master, which distributes work across workers
    # num_stages and num_tasks control the Spark job parallelism
    local cmd="cd ~/mp4_g82 && ~/spark/bin/spark-submit --master $master_url --conf 'spark.driver.extraJavaOptions=-XX:+IgnoreUnrecognizedVMOptions' --conf 'spark.executor.extraJavaOptions=-XX:+IgnoreUnrecognizedVMOptions' scripts/spark_job.py $master_url $num_stages $num_tasks"
    
    for arg in "$@"; do
        cmd="$cmd '$(echo "$arg" | sed "s/'/'\\\\''/g")'"
    done
    
    cmd="$cmd 2>&1 | grep -v 'WARNING: Using incubator modules' || true"
    
    ssh -T -i "$SSH_KEY_PATH" "$vm_host" "$cmd"
}

verify_install() {
    echo "Verifying Spark installation..."
    for i in $(seq 1 $SERVER_COUNT); do
        local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
        if ssh -T -i "$SSH_KEY_PATH" -o ConnectTimeout=5 "$vm_host" "[ -f ~/spark/bin/spark-submit ]" 2>/dev/null; then
            local version=$(ssh -T -i "$SSH_KEY_PATH" -o ConnectTimeout=5 "$vm_host" "~/spark/bin/spark-submit --version 2>&1 | head -1" 2>/dev/null)
            echo "[VM $i] OK - $version"
        else
            echo "[VM $i] FAILED"
        fi
    done
}

upload_dataset() {
    local dataset_file=$1
    if [ -z "$dataset_file" ]; then
        echo "Usage: $0 upload_dataset <dataset_file>"
        echo "Example: $0 upload_dataset dataset1.csv"
        exit 1
    fi
    
    if [ ! -f "$dataset_file" ]; then
        echo "Error: Dataset file '$dataset_file' does not exist."
        exit 1
    fi
    
    echo "Uploading $dataset_file to all VMs (Spark workers need file access)..."
    local filename=$(basename "$dataset_file")
    for i in $(seq 1 $SERVER_COUNT); do
        local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
        echo "[VM $i] Deleting existing file (if any) and uploading..."
        ssh -i "$SSH_KEY_PATH" "$vm_host" "rm -f ~/mp4_g82/$filename" 2>/dev/null
        scp -i "$SSH_KEY_PATH" "$dataset_file" "${vm_host}:~/mp4_g82/" &
    done
    wait
    echo "Upload complete: $dataset_file is now on all VMs at ~/mp4_g82/$filename"
}

cleanup_spark() {
    echo "Cleaning up Spark from all VMs..."
    for i in $(seq 1 $SERVER_COUNT); do
        local vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
        ssh -T -i "$SSH_KEY_PATH" "$vm_host" "~/spark/sbin/stop-worker.sh 2>/dev/null; ~/spark/sbin/stop-master.sh 2>/dev/null; pkill -f org.apache.spark 2>/dev/null; rm -rf ~/spark; rm -f ~/spark-*.tgz ~/spark-*" &
    done
    wait
    echo "Cleanup complete"
}

case "${1:-install}" in
    install)
        if [ -n "$2" ]; then
            vm_num=$2
            vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $vm_num).cs.illinois.edu"
            echo "[VM $vm_num] Installing Spark..."
            ssh -T -i "$SSH_KEY_PATH" "$vm_host" "rm -rf ~/spark ~/spark-* ~/spark-*.tgz; mkdir -p ~/spark" 2>/dev/null
            scp -i "$SSH_KEY_PATH" "$SPARK_TARBALL" "${vm_host}:~/spark/"
            install_spark_on_vm "$2"
        else
            echo "Installing Spark sequentially on all VMs..."
            for i in $(seq 1 $SERVER_COUNT); do
                vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
                echo ""
                echo "=== Installing on VM $i ==="
                ssh -T -i "$SSH_KEY_PATH" "$vm_host" "rm -rf ~/spark ~/spark-* ~/spark-*.tgz; mkdir -p ~/spark" 2>/dev/null
                echo "[VM $i] Copying tarball..."
                scp -i "$SSH_KEY_PATH" "$SPARK_TARBALL" "${vm_host}:~/spark/"
                echo "[VM $i] Extracting and configuring..."
                install_spark_on_vm $i
            done
            echo ""
            echo "Installation complete on all VMs!"
            verify_install
        fi
        ;;
    start)
        start_master
        sleep 2
        start_workers
        echo "Spark cluster started"
        ;;
    stop)
        for i in $(seq 1 $SERVER_COUNT); do
            vm_host="${SSH_USERNAME}@${SERVER_PREFIX}$(printf "%02d" $i).cs.illinois.edu"
            ssh -T -i "$SSH_KEY_PATH" "$vm_host" "~/spark/sbin/stop-worker.sh 2>/dev/null; ~/spark/sbin/stop-master.sh 2>/dev/null" &
        done
        wait
        echo "Cluster stopped"
        ;;
    run)
        shift  # Remove 'run'
        run_job "$@"
        ;;
    verify)
        verify_install
        ;;
    upload_dataset)
        shift
        upload_dataset "$@"
        ;;
    cleanup)
        cleanup_spark
        ;;
    *)
        echo "Usage: $0 {install [vm_num]|start|stop|run [args]|verify|upload_dataset <file>|cleanup}"
        exit 1
        ;;
esac
