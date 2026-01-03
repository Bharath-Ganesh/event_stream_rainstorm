#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys
import time

def apply_filter(df, pattern, col_idx=None):
    """Filter operation (like op1_filter): search entire row, col_idx is only for key extraction in RainStorm"""
    if pattern:
        from pyspark.sql.functions import concat_ws
        # Use backticks to escape column names with special characters
        return df.filter(concat_ws("", *[col(f"`{c}`") for c in df.columns]).contains(pattern))
    return df

def apply_count(df, key_col_arg):
    """Count operation (like op2_count): group by column index and count"""
    idx = int(key_col_arg)
    key_col = df.columns[idx]
    return df.groupBy(key_col).agg(count("*").alias("count"))

def apply_identity(df):
    """Identity operation (like op0_identity): pass through all rows"""
    return df

def apply_transform(df):
    """Transform operation (like op3_transform): output first 3 columns only"""
    if len(df.columns) >= 3:
        return df.select([col(f"`{c}`") for c in df.columns[:3]])
    raise ValueError(f"DataFrame has less than 3 columns: {len(df.columns)}")

def main():
    if len(sys.argv) < 8:
        print("Usage: spark_job.py <master_url> <Nstages> <Ntasks> <op1> <op1_args> [op2] [op2_args] <input_file> [output_file]")
        print("Example: spark_job.py spark://host:7077 2 3 filter 'Parking 8' count '8' dataset1.csv output.csv")
        sys.exit(1)
    
    master_url = sys.argv[1]
    num_stages = int(sys.argv[2])
    num_tasks = int(sys.argv[3])
    op1 = sys.argv[4]
    op1_args = sys.argv[5]
    
    arg_idx = 6
    op2 = None
    op2_args = None
    # Parse second operation and args if num_stages >= 2
    # Example: sys.argv = [..., "filter", "Parking 8", "count", "8", "dataset1.csv"]
    #          -> op2 = "count", op2_args = "8", arg_idx = 8
    if num_stages >= 2 and len(sys.argv) > arg_idx:
        op2 = sys.argv[arg_idx]
        op2_args = sys.argv[arg_idx + 1] if len(sys.argv) > arg_idx + 1 else ""
        arg_idx += 2
    
    if len(sys.argv) <= arg_idx:
        print("Error: input_file is required")
        sys.exit(1)
    
    input_file = sys.argv[arg_idx]
    output_file = sys.argv[arg_idx + 1] if len(sys.argv) > arg_idx + 1 else "spark_output"
    
    # Create SparkSession: connects to master, sets app name, configures parallelism
    # master_url: e.g., "spark://host:7077" (cluster master node)
    # shuffle.partitions: controls number of partitions for shuffle operations (parallelism = num_tasks)
    spark = SparkSession.builder \
        .appName("RainStormComparison") \
        .master(master_url) \
        .config("spark.sql.shuffle.partitions", num_tasks) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        df = spark.read.csv(input_file, header=True, inferSchema=True)
        df = df.repartition(num_tasks)
        input_count = df.count()
        print(f"Input rows: {input_count}")
        
        # Track job start time for throughput measurement (after input is read, similar to RainStorm source start)
        job_start_time = time.time()
        
        if op1 == "filter":
            parts = op1_args.strip().rsplit(" ", 1)
            if len(parts) == 2 and parts[1].isdigit():
                pattern = parts[0]
                col_idx = int(parts[1])
                df = apply_filter(df, pattern, col_idx)
                filtered_count = df.count()
                print(f"After filter '{pattern}': {filtered_count} rows")
            else:
                df = apply_filter(df, op1_args, None)
                filtered_count = df.count()
                print(f"After filter '{op1_args}': {filtered_count} rows")
        elif op1 == "identity":
            df = apply_identity(df)
        elif op1 == "transform":
            df = apply_transform(df)
        else:
            raise ValueError(f"Unknown operation: {op1}")
        
        if num_stages >= 2 and op2:
            if op2 == "count":
                stage2_input = df.count()
                print(f"Stage 2 input: {stage2_input} rows")
                df = apply_count(df, op2_args)
                count_result = df.count()
                print(f"After count: {count_result} groups")
            elif op2 == "filter":
                parts = op2_args.strip().rsplit(" ", 1)
                if len(parts) == 2 and parts[1].isdigit():
                    pattern = parts[0]
                    col_idx = int(parts[1])
                    df = apply_filter(df, pattern, col_idx)
                else:
                    df = apply_filter(df, op2_args, None)
            elif op2 == "transform":
                stage2_input = df.count()
                print(f"Stage 2 input: {stage2_input} rows")
                df = apply_transform(df)
                transform_result = df.count()
                print(f"After transform: {transform_result} rows, {len(df.columns)} columns")
            else:
                raise ValueError(f"Unknown operation: {op2}")
        
        final_df = df
        final_count = final_df.count()
        print(f"Final DataFrame before write: {final_count} rows")
        
        if output_file:
            import os
            spark_output_path = os.path.expanduser(f"~/spark/{output_file}")
            print(f"Writing output to: {spark_output_path}")
            import subprocess
            subprocess.run(["rm", "-rf", spark_output_path], check=False)
            
            if final_count > 0:
                print(f"Attempting to write {final_count} rows...")
                # Collect data to driver and write using Python CSV to ensure file is on VM1
                import csv
                os.makedirs(spark_output_path, exist_ok=True)
                output_file_path = os.path.join(spark_output_path, "part-00000.csv")
                rows = final_df.collect()
                with open(output_file_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    # Write header
                    writer.writerow(final_df.columns)
                    # Write rows
                    for row in rows:
                        writer.writerow(row)
                # Create _SUCCESS file
                success_file = os.path.join(spark_output_path, "_SUCCESS")
                open(success_file, 'a').close()
                print(f"Output written: 1 part file with {len(rows)} rows")
            else:
                print(f"Warning: DataFrame is empty, no output written")
        
        # Calculate and print throughput
        job_end_time = time.time()
        elapsed_time = job_end_time - job_start_time
        total_tuples = final_count
        throughput = total_tuples / elapsed_time if elapsed_time > 0 else 0.0
        
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_end_time))
        print(f"[{timestamp}] [THROUGHPUT] Total={total_tuples}, Time={elapsed_time:.3f}s, Throughput={throughput:.2f} tuples/sec")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
