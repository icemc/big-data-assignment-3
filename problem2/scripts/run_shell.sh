#!/bin/bash

# Configuration - edit these values as needed
SPARK_SCRIPT_DIRECTORY="../src/main/scala"
SPARK_SCRIPT="WikipediaMoviePlotLSA" # Path to your spark Script
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G --conf spark.default.parallelism=8 --conf spark.sql.shuffle.partitions=8 --conf spark.memory.fraction=0.8"  # Spark configuration
LIB_DIR="../lib"

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

# Validate spark Script exists
if [ ! -f "$SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT" ]; then
  echo "Error: Script file not found at $SPARK_SCRIPT"
  echo "Please edit the SPARK_SCRIPT variable in this script"
  exit 1
fi




cp $SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT .

# Create log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/${TIMESTAMP}.log"

# Record start time
START_TIME=$(date +%s)

# Run Spark job
echo "Running LSA on Wikipedia movie plots analysis"
echo "  Spark Script file:    $(realpath $SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT)"

{
spark-shell \
  --jars "$(echo $LIB_DIR/*.jar | tr ' ' ',')" \
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
} 2>&1 | filter_spark_logs | tee "$LOG_FILE"


# Cleanup
rm $SPARK_SCRIPT