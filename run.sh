#!/bin/bash

SRC_DIR="pollution_analysis/src"
PKG_DIR="$SRC_DIR/mapred"

JAR_NAME="project.jar"
MAIN_CLASS="mapred.Main"

HDFS_INPUT="/input/daily_data"

HDFS_PREP="/output/daily_prep"
HDFS_ANNUAL="/output/state_annual_average"
HDFS_US="/output/us_annual_average"

LOCAL_BASE="Downloads/output"
LOCAL_PREP="$LOCAL_BASE/daily_prep"
LOCAL_ANNUAL="$LOCAL_BASE/state_annual_average"
LOCAL_US="$LOCAL_BASE/us_annual_average"

hadoop com.sun.tools.javac.Main "$PKG_DIR"/*.java
jar cf "$JAR_NAME" -C "$SRC_DIR" .

hadoop fs -rm -r -f "$HDFS_PREP" "$HDFS_ANNUAL" "$HDFS_US"

# Run hadoop with YARN mapreduce framework

hadoop jar "$JAR_NAME" "$MAIN_CLASS" -D mapreduce.framework.name=yarn \
  "$HDFS_INPUT" "$HDFS_PREP" "$HDFS_ANNUAL" "$HDFS_US"

rm -rf "$LOCAL_PREP" "$LOCAL_ANNUAL" "$LOCAL_US"
mkdir -p "$LOCAL_PREP" "$LOCAL_ANNUAL" "$LOCAL_US"

hadoop fs -get "$HDFS_PREP"/*   "$LOCAL_PREP/"
hadoop fs -get "$HDFS_ANNUAL"/* "$LOCAL_ANNUAL/"
hadoop fs -get "$HDFS_US"/*     "$LOCAL_US/"

echo "SUCCESS"