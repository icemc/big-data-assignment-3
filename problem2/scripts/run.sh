#!/bin/bash

# Configuration - edit these values as needed
JAR_FILE="../output/WikipediaMoviePlotsLSA.jar"          # Path to your compiled JAR file
DEFAULT_INPUT="../../input/movie_plots/wiki_movie_plots_deduped.csv"   # Default input file
DEFAULT_STOPWORDS="../../input/stopwords.txt" # Default stopwords file
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration
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

# Get input directory from command line or use default
INPUT_DIR=${1:-$DEFAULT_INPUT}
STOPWORDS_PATH=${2:-$DEFAULT_STOPWORDS}

# Validate JAR file exists
if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found at $JAR_FILE"
  echo "Please edit the JAR_FILE variable in this script"
  exit 1
fi

# Validate input directory exists
if [ ! -f "$INPUT_DIR" ]; then
  echo "Error: Input file not found at $INPUT_DIR"
  echo "Please either:"
  echo "1) Pass a valid file as argument, or"
  echo "2) Edit the DEFAULT_INPUT variable in this script"
  exit 1
fi

# Validate stopwords file exists
if [ ! -f "$STOPWORDS_PATH" ]; then
  echo "Error: Stopwords file not found at $STOPWORDS_PATH"
  echo "Please either:"
  echo "1) Pass a valid stopwords file path as argument, or"
  echo "2) Edit the DEFAULT_STOPWORDS variable in this script"
  exit 1
fi


# Create log file with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/${TIMESTAMP}.log"

# Record start time
START_TIME=$(date +%s)

# Run Spark job and log output
{
echo "Running LSA on Wikipedia movie plots analysis with parameters:"
echo "  JAR file:      $(realpath $JAR_FILE)"
echo "  Data path:     $INPUT_DIR"
echo "  Stopwords:     $(realpath $STOPWORDS_PATH)"
echo "  Spark opts:    $SPARK_OPTS"
echo "----------------------------------------"

spark-submit \
  --class WikipediaMoviePlotsLSA \
  $SPARK_OPTS \
  $JAR_FILE \
  $INPUT_DIR \
  $STOPWORDS_PATH \

# Calculate runtime
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))


echo "----------------------------------------"
echo "Runtime: $RUNTIME seconds"
} 2>&1 | filter_spark_logs | tee "$LOG_FILE"