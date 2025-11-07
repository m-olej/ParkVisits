#!/usr/bin/env bash

set -e

if [ "$#" -ne 2 ]; then
  echo "usage: $0 input_dir1 output_dir3"
  exit 1
fi


INPUT_DIR1=$1
OUTPUT_DIR3=$2

echo "Cleaning output dir"
hdfs dfs -rm -r -f "$OUTPUT_DIR3"

echo "Running MapReduce"

hadoop jar parkvisits.jar "$INPUT_DIR1" "$OUTPUT_DIR3"