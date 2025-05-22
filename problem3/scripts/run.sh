#!/bin/bash

# Configuration - edit these values as needed
JAR_FILE="../output/GearboxOutlierDetection.jar"          # Path to your compiled JAR file
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration

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

# Record start time
START_TIME=$(date +%s)

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
