#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
LIB_DIR="$PROJECT_ROOT/lib"
OUTPUT_DIR="$PROJECT_ROOT/output"
JAR_FILE="$OUTPUT_DIR/RunLSA.jar"

# Configuration
DEFAULT_MODEL_DIR="$PROJECT_ROOT/model"
SPARK_OPTS="--master local[*] --executor-memory 8G --driver-memory 8G"
LOG_DIR="search"
mkdir -p "$LOG_DIR"

# Help function
usage() {
  echo "Usage: $0 <modelDir> <queries>"
  echo "  modelDir:   Path to directory containing LSA model files"
  echo "  queries:    Comma-separated list of queries to process (required)"
  echo ""
  echo "Required model files:"
  echo "  US/          - Term-document matrix directory"
  echo "  V.ser        - Serialized term-concept matrix"
  echo "  idTerms.ser  - Serialized term ID mapping"
  echo "  idfs.ser     - Serialized IDF weights"
  echo "  docIds.ser   - Serialized document ID mapping"
  echo ""
  echo "Examples:"
  echo "  $0 /path/to/model \"data science,machine learning\""
  echo "  $0 ../my_model \"apache spark\""
  exit 1
}

# Filter Spark logs
filter_spark_logs() {
  while IFS= read -r line; do
    if [[ ! "$line" =~ ^[0-9]{2}/[0-9]{2}/[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2} ]]; then
      echo "$line"
    fi
  done
}

# Check for help
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
fi

# Validate parameters
if [ $# -lt 2 ]; then
  echo "Error: Both model directory and queries must be provided"
  usage
fi

MODEL_DIR=$1
QUERIES=$2

# Validate JAR exists
if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found at $JAR_FILE"
  echo "Please run build.sh first to compile the project"
  exit 1
fi

# Validate model directory
if [ ! -d "$MODEL_DIR" ]; then
  echo "Error: Model directory not found at $MODEL_DIR"
  usage
fi

# Validate model files
REQUIRED_ITEMS=("US" "V.ser" "idTerms.ser" "idfs.ser" "docIds.ser")
echo "Validating model directory contents..."
for item in "${REQUIRED_ITEMS[@]}"; do
  item_path="$MODEL_DIR/$item"

  if [ "$item" == "US" ]; then
    if [ ! -d "$item_path" ]; then
      echo "Error: Required directory 'US' not found in $MODEL_DIR"
      usage
    fi
  elif [ ! -f "$item_path" ]; then
    echo "Error: Required file $item not found in $MODEL_DIR"
    usage
  fi
done
echo "✓ Model validation passed"

# Prepare logging
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/search_${TIMESTAMP}.log"
START_TIME=$(date +%s)

# Execute Spark job
{
echo "Starting LSA Search Engine"
echo "  Model dir:  $(realpath "$MODEL_DIR")"
echo "  Queries:    $QUERIES"
echo "  Spark opts: $SPARK_OPTS"
echo "----------------------------------------"

"$SPARK_HOME/bin/spark-submit" \
  --class "SearchEngine" \
  --jars "$(echo "$LIB_DIR"/*.jar | tr ' ' ',')" \
  $SPARK_OPTS \
  "$JAR_FILE" \
  "$MODEL_DIR" \
  "$QUERIES"

RUNTIME=$(($(date +%s) - START_TIME))
echo "----------------------------------------"
echo "Completed in $RUNTIME seconds"
} 2>&1 | filter_spark_logs | tee "$LOG_FILE"

exit 0