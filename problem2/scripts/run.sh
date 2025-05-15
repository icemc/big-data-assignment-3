#!/bin/bash

# Configuration - edit these values as needed
JAR_FILE="../output/WeatherPrediction.jar"          # Path to your compiled JAR file
DEFAULT_INPUT="../../input/NOAA-065900"   # Default input directory
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration

# Get input directory from command line or use default
INPUT_DIR=${1:-$DEFAULT_INPUT}

# Validate JAR file exists
if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found at $JAR_FILE"
  echo "Please edit the JAR_FILE variable in this script"
  exit 1
fi

# Validate input directory exists
if [ ! -d "$INPUT_DIR" ]; then
  echo "Error: Input directory not found at $INPUT_DIR"
  echo "Please either:"
  echo "1) Pass a valid directory as argument, or"
  echo "2) Edit the DEFAULT_INPUT variable in this script"
  exit 1
fi

# Run Spark job
echo "Running weather prediction analysis with:"
echo "  JAR file:    $(realpath $JAR_FILE)"
echo "  Input data:  $(realpath $INPUT_DIR)"
echo "  Spark opts:  $SPARK_OPTS"

spark-submit \
  --class WeatherPrediction \
  $SPARK_OPTS \
  $JAR_FILE \
  $INPUT_DIR

echo "Analysis complete. Results saved to report.txt"