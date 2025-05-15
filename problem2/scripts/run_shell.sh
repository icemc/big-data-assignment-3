#!/bin/bash

# Configuration - edit these values as needed
SPARK_SCRIPT_DIRECTORY="../src/main/scala"
SPARK_SCRIPT="WeatherPrediction"          # Path to your spark Script
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration


# Validate spark Script exists
if [ ! -f "$SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT" ]; then
  echo "Error: Script file not found at $SPARK_SCRIPT"
  echo "Please edit the SPARK_SCRIPT variable in this script"
  exit 1
fi


# Run Spark job
echo "Running weather prediction analysis with:"
echo "  Spark Script file:    $(realpath $SPARK_SCRIPT_DIRECTORY/$SPARK_SCRIPT)"

cd $SPARK_SCRIPT_DIRECTORY || exit 1

spark-shell \
  $SPARK_OPTS \
  -i \
  $SPARK_SCRIPT \

cp report.txt ../../../scripts

echo "Analysis complete. Results saved to report.txt"