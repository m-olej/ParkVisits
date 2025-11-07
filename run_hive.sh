#!/usr/bin/env bash

set -e

if [ "$#" -ne 3 ]; then
  echo "usage: ./$0 input_dir3 input_dir4 output_dir6"
  exit 1
fi

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3

echo "Cleaning output dir"
hdfs dfs -rm -r -f "$OUTPUT_DIR6"

HIVE_JDBC_URL="jdbc:hive2://localhost:10000/default"

echo "Starting Hive Beeline job..."

beeline -u $HIVE_JDBC_URL -n "$USER" \
    -f hive.hql \
    --hiveconf INPUT_DIR3="$INPUT_DIR3" \
    --hiveconf INPUT_DIR4="$INPUT_DIR4" \
    --hiveconf OUTPUT_DIR6="$OUTPUT_DIR6"