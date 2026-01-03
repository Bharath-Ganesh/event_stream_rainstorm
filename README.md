# MP4 Stream Processing
Team Members:
- bganesh2
- yonghan4

## Go into root directory
```bash
cd mp4_g82
```
## Add Introducer
```bash
# VM1 is our introducer 
# commandport is fixed to 9080
go run ./mp4_main -port 9090 -commandport 9080
```
## Mass Join
```bash
# 9080 is the commandport
# The number list after the port means which VM you want to start
# e.g. 2 4 5 means start to join VM2, VM4, VM5
# the config file is needed to put into the command
./scripts/run.sh mass_join config_demo.json  9080 2 3 4 5 6 7 8 9 10
```

## Join one VM
```bash
# If the machine you want to join is not introducer
# You have to give the introducer address
go run ./mp4_main/ -port 8081 -introducer fa25-cs425-8201.cs.illinois.edu:9090 -commandport 9080
```

## Mass Crash
```bash
# The number list means which VM you want to crash
# It will not shut down the VM, it just kill the process in the VMS
# e.g. 2 4 5 means start to crash VM2, VM4, VM5
# the config file is needed to put into the command
./scripts/run.sh mass_crash config_demo.json 2 3 4 5 6 7 8 9 10
```

## Create HyDFS Command
Create a file on HyDFS and copy the contents of localfilename from local dir
```
create localfilename HyDFSfilename
```
Example Command  
```
go run ./demo/ create business_2.txt b2.txt
```
Example Result  
```
Create Command complete.
VM6 (172.22.95.203:9081)
VM3 (172.22.95.202:9081)
VM10 (172.22.155.19:9081)
```

## Get HyDFS Command
Fetches the entire file from HyDFS to localfilename on local dir
```
get HyDFSfilename localfilename
```
Example Command  
```
go run ./demo/ get b2.txt local_b2.txt
```

Example Result
```
Get command finish.
```

## ls HyDFSfilename
List all machine (VM) addresses (along with the VMs’ IDs on the ring) where this file is currently being stored. Also prints the fileID of HyDFSfilename.

Example Command
```
go run ./demo/ ls b2.txt
```

Example Result
```
hyDFSFileName b2.txt FileID: 4095064915
Replicas-------------
RingID 4150215520 in VM9 (172.22.95.204:9081)
RingID 266348224 in VM3 (172.22.95.202:9081)
RingID 529049713 in VM4 (172.22.155.17:9081)
```

## list_tasks
query the leader process for task details. For each task process, output its VM, PID, op_exe, and local log file.

Example Command
```bash
go run ./demo list_tasks
```

Example Result
```bash
Stage | Task | VM Address                | PID     | Op Exe          | Log File
----------------------------------------------------------------------------------------------------
    1 |    0 | VM4(172.22.155.17:9082)   |   44711 | op1_filter      | slog/rs_Stage1_Task0_20251205_223439.log
    1 |    1 | VM7(172.22.155.18:9082)   |   44191 | op1_filter      | slog/rs_Stage1_Task1_20251205_223439.log
    1 |    2 | VM10(172.22.155.19:9082)  |   44119 | op1_filter      | slog/rs_Stage1_Task2_20251205_223439.log
    2 |    0 | VM2(172.22.159.16:9082)   |   43935 | op2_count       | slog/rs_Stage2_Task0_20251205_223439.log
    2 |    1 | VM5(172.22.159.17:9082)   |   44325 | op2_count       | slog/rs_Stage2_Task1_20251205_223439.log
    2 |    2 | VM8(172.22.159.18:9082)   |   43917 | op2_count       | slog/rs_Stage2_Task2_20251205_223439.log
```

## kill_task
given a task’s VM and PID, abruptly kill the task process.  Send a kill -9 or similar - not a graceful shutdown.

Example Command
```bash
go run ./demo kill_task VM5 5952
```

Example Result
```bash
Sending KillTask command to VM5 for PID 5952...
Kill Task Success.
```

## Build Op Exe
```bash
# the first parameter is binary file, the second is the .go file you want to be packaged to
./vm_deploy.sh build op0_identity .ops/identity/op0_identity.go
./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go
./vm_deploy.sh build op2_count ./ops/count/op2_count.go
./vm_deploy.sh build op3_transform ./ops/count/op3_transform.go
```

## RainStorm

```bash
RainStorm numStages numTasks op1_exe op1_args src_file dest_file exactly-once autoscale [input_rate] [lw] [hw]
```

### Example: Test 0 with autoscale = false
```bash
go run ./demo RainStorm 1 3 op0_identity "" dataset1.csv output_app0 true false
```

### Example: Test 1 with autoscale = false
```bash
go run ./demo RainStorm 2 3 op1_filter "Parking 8" op2_count "8" dataset1.csv output_test1 true false
```

## Commands for test 0

### In VM1, start the leader
```bash
go run ./mp4_main -port 9090 -commandport 9080
```

### In local, start all the other nodes and build op0_identity for all the VMs
```bash
# join other VMs (VM2-10)
./scripts/run.sh mass_join config_demo.json  9080 2 3 4 5 6 7 8 9 10
# build binary file for all VMs
./vm_deploy.sh build op0_identity ./ops/identity/op0_identity.go
```

### Create HYDFS file before starting RainStorm
```bash
go run ./demo create dataset1.csv dataset1.csv
```

### RainStorm
```bash
go run ./demo RainStorm 1 3 op0_identity "" dataset1.csv output_app0 true false
```

### Get dest output HYDFS
```bash
go run ./demo get output_app0 local_dataset1.csv
```

### Sort and Compare
```bash
# sort the original dataset
sort dataset1.csv > sorted_original.csv
# sort the local file from streaming output file
sort local_dataset1.csv > sorted_output.csv
# check difference
diff sorted_original.csv sorted_output.csv 
```

## Commands for test 1

### In VM1, start the leader
```bash
go run ./mp4_main -port 9090 -commandport 9080
```

### In local, start all the other nodes and build op0_identity for all the VMs
```bash
# join other VMs (VM2-10)
./scripts/run.sh mass_join config_demo.json  9080 2 3 4 5 6 7 8 9 10
# build binary file for all VMs
./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go
./vm_deploy.sh build op2_count ./ops/count/op2_count.go
```

### Create HYDFS file before starting RainStorm
```bash
go run ./demo create dataset1.csv dataset1.csv
```

### RainStorm
```bash
go run ./demo RainStorm 2 3 op1_filter "Parking 8" op2_count "8" dataset1.csv output_test1 true false
```

### Check1
The outputs should be continuously written to the output file on HyDFS. 
#### monitoring the tasks’ local log files
```bash
# list the tasks first
go run ./demo list_tasks
# go to one of the VM
tail -f slog/rs_Stage1_*.log
```

#### Output file’s contents on HyDFS
```bash
# list the replicas of the HyDFS
go run ./demo ls output_test1
# go to one of the replica
tail -f hydfs/output_test1
```

### Check2 Correctness

```bash
# Use local command to get from HyDFS
go run ./demo get output_test1 local_output
# Check the total line count (It should be 515 in this case)
wc -l local_output

# verify by specific key counts
grep "NE" local_output | tail -n 1
# Expected: NE: 116
grep "NW" local_output | tail -n 1
# Expected: NW: 106
```

### Check3 Performance
```bash
# Go to leader VM1
# You should get the time for starting source stage, time for end of the source stage, and rainStorm completion time
grep -E "\[JOB\]" slog/rs_leader_*.log
```

#### Example output for check3
```bash
[yonghan4@fa25-cs425-8201 mp4_g82]$ grep -E "\[JOB\]" slog/rs_leader_*.log
[JOB] 2025/12/05 22:34:39.410417 Starting Job: Stages=2, Tasks=3, Src=dataset1.csv, EO=true
[JOB] 2025/12/05 22:35:09.566256 End of Entire Run (Source finished reading input)
[JOB] 2025/12/05 22:35:09.571231 RAINSTORM JOB COMPLETED: All 6 tasks finished successfully.
```

## Commands for Test2

### Clean OP Exe from last test
```bash
./vm_deploy.sh clean_op_tasks
```

### RainStorm
```bash
go run ./demo RainStorm 2 3 op1_filter "Parking 8" op2_count "8" dataset1.csv output_test2 true false
```

### List Tasks
```bash
go run ./demo list_tasks
```

### Kill Task
Kill one of the task in Stage 1
```bash
# VM4 (VMName), 1234 (PID you want to kill)
go run ./demo kill_task VM4 1234
```

### Check for Test 2
```bash
go run ./demo get output_test2 local_output2
sort local_output2 | uniq -c
# We expect to see '1' at the start of every line
sort local_output2 | uniq -d
# No Duplicate
sort local_output > sorted_1
sort local_output2 > sorted_2
diff sorted_1 sorted_2
# It should be same 
```

## Commands for test 3

### Clean OP Exe from last test
```bash
./vm_deploy.sh clean_op_tasks
```

### put local dataset2.csv to HYDFS
```bash
go run ./demo create dataset2.csv dataset2.csv
```

### Make sure you have build op1_filter op3_transform
```bash
./vm_deploy.sh build op1_filter ./ops/filter/op1_filter.go
./vm_deploy.sh build op3_transform ./ops/count/op3_transform.go
```

### Execute rainStorm autoscaling
```bash
# the last three parameters are input_rate, LW, HW
go run ./demo RainStorm 2 3 op1_filter "SIGN_POLE" op3_transform "" dataset2.csv output_test3 false true 100 4 9
```

### Fetch HyDFS to local file
```bash
go run ./demo get output_test3 local_test3
```

### Calculate the number of unique lines (Prove at-least-once)
```bash
sort local_test3 | uniq | wc -l
# Expected: 2044
```

### Try different input_rate, LW, HW
```bash
go run ./demo RainStorm 2 3 op1_filter "SIGN_POLE" op3_transform "" dataset2.csv output_test3_2 false true 200 8 15
go run ./demo get output_test3_2 local_test3_2
sort local_test3_2 | uniq | wc -l
# Expected: 2044
```

### Fetch the leader log with JOB related message
```bash
[yonghan4@fa25-cs425-8201 mp4_g82]$ grep -E "\[JOB\]" slog/rs_leader_*.log
[JOB] 2025/12/05 22:16:34.470588 Starting Job: Stages=2, Tasks=3, Src=dataset2.csv, EO=true
[JOB] 2025/12/05 22:17:19.621429 End of Entire Run (Source finished reading input)
[JOB] 2025/12/05 22:17:19.624919 RAINSTORM JOB COMPLETED: All 8 tasks finished successfully.
```
### Fetch the leader log with AutoScale decision
```bash
[yonghan4@fa25-cs425-8201 mp4_g82]$ grep -E "\[AutoScale\]" slog/rs_leader_*.log
[AutoScale] 2025/12/05 22:16:35.500397 [UP] Stage 2 Avg Rate 18.00 > HW 15. Triggering Scale UP.
[AutoScale] 2025/12/05 22:16:37.500173 [UP] Stage 2 Avg Rate 16.75 > HW 15. Triggering Scale UP.
[AutoScale] 2025/12/05 22:16:50.500270 [DOWN] Stage 2 Avg Rate 7.80 < LW 8. Triggering Scale DOWN.
[AutoScale] 2025/12/05 22:16:54.500453 [DOWN] Stage 2 Avg Rate 4.25 < LW 8. Triggering Scale DOWN.
[AutoScale] 2025/12/05 22:16:58.500438 [DOWN] Stage 2 Avg Rate 4.00 < LW 8. Triggering Scale DOWN.
[AutoScale] 2025/12/05 22:17:02.500406 [DOWN] Stage 2 Avg Rate 6.50 < LW 8. Triggering Scale DOWN.
[AutoScale] 2025/12/05 22:17:05.500589 [UP] Stage 2 Avg Rate 63.00 > HW 15. Triggering Scale UP.
[AutoScale] 2025/12/05 22:17:06.500729 [UP] Stage 2 Avg Rate 29.00 > HW 15. Triggering Scale UP.
[AutoScale] 2025/12/05 22:17:08.500644 [UP] Stage 2 Avg Rate 21.33 > HW 15. Triggering Scale UP.
[AutoScale] 2025/12/05 22:17:14.500485 [UP] Stage 2 Avg Rate 17.00 > HW 15. Triggering Scale UP.
```

### Fetch the leader log with ScaleDown Task
```bash
[yonghan4@fa25-cs425-8201 mp4_g82]$ grep "\[Task\].*\[ScaleDown\]" slog/rs_leader_*.log
[Task] 2025/12/05 22:16:50.500905 [ScaleDown] Stage 2 Task 4 on VM6 (172.22.95.203:9082) (PID 42917) | Exe: op3_transform | Orphan: dedup_rs-1764994594_stage2_task4_1764994597500252670
[Task] 2025/12/05 22:16:54.500510 [ScaleDown] Stage 2 Task 3 on VM3 (172.22.95.202:9082) (PID 44459) | Exe: op3_transform | Orphan: dedup_rs-1764994594_stage2_task3_1764994595500818866
[Task] 2025/12/05 22:16:58.500502 [ScaleDown] Stage 2 Task 2 on VM8 (172.22.159.18:9082) (PID 42522) | Exe: op3_transform | Orphan: dedup_rs-1764994594_stage2_task2_1764994594470785250
[Task] 2025/12/05 22:17:02.500731 [ScaleDown] Stage 2 Task 1 on VM5 (172.22.159.17:9082) (PID 42932) | Exe: op3_transform | Orphan: dedup_rs-1764994594_stage2_task1_1764994594470783935
```

You can use the same logic for ScaleUp, Start, End Task
```bash
grep "\[Task\].*\[ScaleUp\]" slog/rs_leader_*.log
grep "\[Task\].*\[Start\]" slog/rs_leader_*.log
grep "\[Task\].*\[End\]" slog/rs_leader_*.log
```

### Leader Terminal Message
```
[yonghan4@fa25-cs425-8201 mp4_g82]$ go run ./mp4_main/ -port 9090 -commandport 9080
This is the introducer
[Leader] Received RainStorm Job rs-1765054673 request. Scheduling...
[Leader] Autoscaling ENABLED (Rate=100, LW=4, HW=9)
[Autoscaler] Monitoring started.
[Source] Starting stream file: dataset2.csv, Rate: 100
[Source] Trying to download from 172.22.159.16:9082...
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 3 on VM3 (172.22.95.202:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 4 on VM6 (172.22.95.203:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 5 on VM9 (172.22.95.204:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 6 on VM4 (172.22.155.17:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 7 on VM7 (172.22.155.18:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 8 on VM10 (172.22.155.19:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 9 on VM2 (172.22.159.16:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 10 on VM5 (172.22.159.17:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 1 Task 11 on VM8 (172.22.159.18:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 3 on VM3 (172.22.95.202:9082) (Least Loaded)
[Autoscale] <<< Scaling Down <<< Killing Stage 2 Task 3 on VM3 (172.22.95.202:9082)
[Autoscale] Orphan Adoption Started Successfully for dedup_rs-1765054673_stage2_task3_1765054692324762394
[Autoscale] <<< Scaling Down <<< Killing Stage 2 Task 2 on VM8 (172.22.159.18:9082)
[Autoscale] Orphan Adoption Started Successfully for dedup_rs-1765054673_stage2_task2_1765054673300244097
[Autoscale] <<< Scaling Down <<< Killing Stage 2 Task 1 on VM5 (172.22.159.17:9082)
[Autoscale] Orphan Adoption Started Successfully for dedup_rs-1765054673_stage2_task1_1765054673300242923
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 1 on VM5 (172.22.159.17:9082) (Least Loaded)
[Autoscale] <<< Scaling Down <<< Killing Stage 2 Task 1 on VM5 (172.22.159.17:9082)
[Autoscale] Orphan Adoption Started Successfully for dedup_rs-1765054673_stage2_task1_1765054723324758467
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 1 on VM5 (172.22.159.17:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 2 on VM8 (172.22.159.18:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 3 on VM3 (172.22.95.202:9082) (Least Loaded)
[Autoscale] >>> Scaling Up >>> Starting Stage 2 Task 4 on VM6 (172.22.95.203:9082) (Least Loaded)
[Source] Streaming finished. Sending EOS...
[Autoscale] Monitoring stopped.
[Leader] RAINSTORM JOB COMPLETED. All tasks finished.
```

## Test 4
Find DedupLog filename in leader log
```bash
grep "DedupLog" slog/rs_leader_*.log
```

## Apache Spark Testing

### Upload Dataset
./scripts/run_spark.sh upload_dataset <dataset.csv>

### Start Spark Cluster
./scripts/run_spark.sh start

### Run Spark Job
# Format: ./scripts/run_spark.sh run <Nstages> <Ntasks> <op1> <op1_args> [op2] [op2_args] <input_file> [output_file]
./scripts/run_spark.sh run 2 3 filter "Parking 8" count "8" dataset1.csv spark_output

### Get Output
./vm_deploy.sh copy_from 1 '~/spark/spark_output/part-00000.csv' "results"