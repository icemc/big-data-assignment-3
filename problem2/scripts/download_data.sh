#!/bin/bash

# Define paths
DOWNLOAD_URL="https://www.kaggle.com/api/v1/datasets/download/jrobischon/wikipedia-movie-plots"
ZIP_FILE="../../movie_plots.zip"
DEST_DIR="../../input"
LIB_DIR="../lib"

# Stanford CoreNLP JARs to download
STANFORD_JARS=(
  "https://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.8.0/stanford-corenlp-3.8.0.jar"
  "https://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.8.0/stanford-corenlp-3.8.0-models.jar"
)

echo "1. Downloading required jar files"

# Create required directories
mkdir -p "$LIB_DIR"

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

echo "1. Downloading required dataset files"

# Check if destination directory exists and contains files
if [ -d "$DEST_DIR/movie_plots" ] && [ -n "$(ls -A "$DEST_DIR/movie_plots" 2>/dev/null)" ]; then
    echo "Destination directory already exists and contains files: $(realpath "$DEST_DIR/movie_plots")"
    echo "Skipping download and extraction."
    exit 0
fi

# Create destination directory (will overwrite existing files during extraction)
mkdir -p "$DEST_DIR"

# Download the zip file
echo "Downloading zip file from $DOWNLOAD_URL..."
wget "$DOWNLOAD_URL" -O "$ZIP_FILE"

# Verify download was successful
if [ $? -ne 0 ]; then
    echo "Failed to download the zip file"
    exit 1
fi

# Use Python to unzip and overwrite existing files
python3 - <<EOF
import zipfile
import os
from pathlib import Path

zip_path = Path("$ZIP_FILE")
dest_dir = Path("$DEST_DIR/movie_plots")

# Open the zip file and extract all files, overwriting existing ones
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    for file in zip_ref.namelist():
        print(f"Extracting: {file}")
        zip_ref.extract(file, dest_dir)

print("Unzipping completed - existing files were overwritten.")
EOF

# Clean up the zip file
rm "$ZIP_FILE"
echo "Removed downloaded zip file: $ZIP_FILE"

echo "Finished extracting files to $DEST_DIR"