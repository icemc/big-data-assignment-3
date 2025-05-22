#!/bin/bash

# Configuration - edit these values as needed
SPARK_SCRIPT_DIRECTORY="../src/main/scala"
SPARK_SCRIPT="GearboxOutlierDetection" # Path to your spark Script
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G --conf spark.default.parallelism=8 --conf spark.sql.shuffle.partitions=8 --conf spark.memory.fraction=0.8"  # Spark configuration


# Validate spark Script exists
if [ ! -f "$SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT" ]; then
  echo "Error: Script file not found at $SPARK_SCRIPT"
  echo "Please edit the SPARK_SCRIPT variable in this script"
  exit 1
fi


# Run Spark job
echo "Running recommendation system analysis with:"
echo "  Spark Script file:    $(realpath $SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT)"

cp $SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT .

# Record start time
START_TIME=$(date +%s)

spark-shell \
  $SPARK_OPTS \
  -i \
  $SPARK_SCRIPT

# Calculate runtime
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))

echo "----------------------------------------"
echo "Analysis complete"
echo "Runtime: $RUNTIME seconds"
echo "----------------------------------------"

# Cleanup
rm $SPARK_SCRIPT