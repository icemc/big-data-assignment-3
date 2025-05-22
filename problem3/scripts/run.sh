#!/bin/bash

# Configuration - edit these values as needed
JAR_FILE="../output/GearboxOutlierDetection.jar"          # Path to your compiled JAR file
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration
OUTPUT_DIR="results"                                 # Directory for log files
mkdir -p "$OUTPUT_DIR"

# Function to filter Spark logs
filter_spark_logs() {
  while IFS= read -r line; do
    # Skip lines with Spark timestamps (##/##/## ##:##:##)
    if [[ ! "$line" =~ ^[0-9]{2}/[0-9]{2}/[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2} ]]; then
      echo "$line"
    fi
  done
}

# Validate JAR file exists
if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found at $JAR_FILE"
  echo "Please edit the JAR_FILE variable in this script"
  exit 1
fi

# Run Spark job
echo "Running gearbox outlier detection analysis with:"
echo "  JAR file:    $(realpath $JAR_FILE)"
echo "  Spark opts:  $SPARK_OPTS"

# Create log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/${TIMESTAMP}.log"

# Record start time
START_TIME=$(date +%s)

{
spark-submit \
  --class GearboxOutlierDetection \
  $SPARK_OPTS \
  $JAR_FILE

# Calculate runtime
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))

echo "----------------------------------------"
echo "Analysis complete"
echo "Runtime: $RUNTIME seconds"
echo "----------------------------------------"
} 2>&1 | filter_spark_logs | tee "$LOG_FILE"