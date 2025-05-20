#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
LIB_DIR="$PROJECT_ROOT/lib"

# Configuration - edit these values as needed
JAR_FILE="../output/RunLSA.jar"                      # Path to your compiled JAR file
DEFAULT_CLASS="RunLSA"                               # Default class to run
DEFAULT_SAMPLE_SIZE="0.01"                           # Default sample size
DEFAULT_NUM_TERMS="5000"                             # Default number of terms
DEFAULT_K="25"                                       # Default number of topics
DEFAULT_DATA_PATH="../../input/wikipedia/articles/*/*" # Default data path
DEFAULT_STOPWORDS="../../input/wikipedia/stopwords.txt" # Default stopwords file
SPARK_OPTS="--master local[*] --executor-memory 16G --driver-memory 16G"  # Spark configuration
OUTPUT_DIR="results"                                 # Directory for log files
mkdir -p "$OUTPUT_DIR"

# Help function
usage() {
  echo "Usage: $0 [class] [sampleSize] [numTerms] [k] [dataPath] [stopwordsPath]"
  echo "  class:           RunLSA (default) or RunLSASimpleTokenizer"
  echo "  sampleSize:      Double (default: $DEFAULT_SAMPLE_SIZE)"
  echo "  numTerms:        Integer (default: $DEFAULT_NUM_TERMS)"
  echo "  k:               Integer (default: $DEFAULT_K)"
  echo "  dataPath:        String (default: $DEFAULT_DATA_PATH)"
  echo "  stopwordsPath:   String (default: $DEFAULT_STOPWORDS)"
  echo ""
  echo "Examples:"
  echo "  $0 RunLSA 0.01 5000 25 \"../../input/wikipedia/articles/*/*\" \"../../input/wikipedia/stopwords.txt\""
  echo "  $0 RunLSASimpleTokenizer 0.05 10000 50 \"/new/data/path\" \"/new/stopwords.txt\""
  exit 1
}

# Function to filter Spark logs
filter_spark_logs() {
  while IFS= read -r line; do
    # Skip lines with Spark timestamps (##/##/## ##:##:##)
    if [[ ! "$line" =~ ^[0-9]{2}/[0-9]{2}/[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2} ]]; then
      echo "$line"
    fi
  done
}

# Check if help is requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
fi

# Get parameters from command line or use defaults
CLASS=${1:-$DEFAULT_CLASS}
SAMPLE_SIZE=${2:-$DEFAULT_SAMPLE_SIZE}
NUM_TERMS=${3:-$DEFAULT_NUM_TERMS}
K=${4:-$DEFAULT_K}
DATA_PATH=${5:-$DEFAULT_DATA_PATH}
STOPWORDS_PATH=${6:-$DEFAULT_STOPWORDS}

# Validate class option
if [[ "$CLASS" != "RunLSA" && "$CLASS" != "RunLSASimpleTokenizer" ]]; then
  echo "Error: Invalid class '$CLASS'. Must be either RunLSA or RunLSASimpleTokenizer"
  usage
fi

# Validate JAR file exists
if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found at $JAR_FILE"
  echo "Please edit the JAR_FILE variable in this script"
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
LOG_FILE="${OUTPUT_DIR}/${CLASS}_sample${SAMPLE_SIZE}_terms${NUM_TERMS}_k${K}_${TIMESTAMP}.log"

# Record start time
START_TIME=$(date +%s)

# Run Spark job and log output
{
echo "Running $CLASS analysis with parameters:"
echo "  JAR file:      $(realpath $JAR_FILE)"
echo "  Class:         $CLASS"
echo "  Sample size:   $SAMPLE_SIZE"
echo "  Num terms:     $NUM_TERMS"
echo "  k (topics):    $K"
echo "  Data path:     $DATA_PATH"
echo "  Stopwords:     $(realpath $STOPWORDS_PATH)"
echo "  Spark opts:    $SPARK_OPTS"
echo "----------------------------------------"

spark-submit \
  --class $CLASS \
  --jars "$(echo $LIB_DIR/*.jar | tr ' ' ',')" \
  $SPARK_OPTS \
  $JAR_FILE \
  $SAMPLE_SIZE \
  $NUM_TERMS \
  $K
#  $DATA_PATH \
#  $STOPWORDS_PATH

# Calculate runtime
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))

echo "----------------------------------------"
echo "Runtime: $RUNTIME seconds"
} 2>&1 | filter_spark_logs | tee "$LOG_FILE"

# Also display output to console
# cat "$LOG_FILE"

exit 0
