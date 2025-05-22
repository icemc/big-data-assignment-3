#!/bin/bash

# Get the absolute path of the script's directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")
LIB_DIR="$PROJECT_ROOT/lib"
OUTPUT_DIR="$PROJECT_ROOT/output"
SRC_DIR="$PROJECT_ROOT/src/main/scala"
FAT_JAR_NAME="WikipediaMoviePlotLSA.jar"

# Stanford CoreNLP JARs to download
STANFORD_JARS=(
  "https://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.8.0/stanford-corenlp-3.8.0.jar"
  "https://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.8.0/stanford-corenlp-3.8.0-models.jar"
)

# Ensure SPARK_HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME is not set."
  exit 1
fi

# Create required directories
mkdir -p "$LIB_DIR" "$OUTPUT_DIR/classes" "$OUTPUT_DIR/tmp"

# Download required JAR files if they don't exist
for jar_url in "${STANFORD_JARS[@]}"; do
  jar_file=$(basename "$jar_url")
  if [ ! -f "$LIB_DIR/$jar_file" ]; then
    echo "Downloading $jar_file..."
    wget -q -P "$LIB_DIR" "$jar_url"
    if [ $? -ne 0 ]; then
      echo "Failed to download $jar_file"
      exit 1
    fi
    echo "Downloaded $jar_file successfully"
  else
    echo "$jar_file already exists, skipping download"
  fi
done

# Construct classpath from Spark JARs and project libs
CLASSPATH=$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')
LIB_CLASSPATH=$(find "$LIB_DIR" -name "*.jar" | tr '\n' ':')
CLASSPATH="${CLASSPATH}${LIB_CLASSPATH}"

# Compile Scala sources
echo "Compiling Scala sources..."
scalac -classpath "$CLASSPATH" "$SRC_DIR"/*.scala -d "$OUTPUT_DIR/classes"

# Copy compiled class files to tmp for fat jar
cp -r "$OUTPUT_DIR/classes/"* "$OUTPUT_DIR/tmp"

# Extract all classes from library jars into tmp using jar tool (no unzip)
for jar_file in "$LIB_DIR"/*.jar; do
  echo "Extracting $(basename "$jar_file")..."
  (cd "$OUTPUT_DIR/tmp" && jar xf "$jar_file")
done

# Create fat JAR
cd "$OUTPUT_DIR/tmp" || exit 1
echo "Creating fat JAR: $FAT_JAR_NAME"
jar -cvf "../$FAT_JAR_NAME" .


# Clean up temporary files
echo "Cleaning up..."
rm -rf "$OUTPUT_DIR/tmp"
rm -rf "$OUTPUT_DIR/classes"

echo "Fat JAR build complete: $OUTPUT_DIR/$FAT_JAR_NAME"
