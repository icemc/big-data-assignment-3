#!/bin/bash

# Define paths
DOWNLOAD_URL="https://archive.org/download/wikipedia_202505/wikipedia.zip"
ZIP_FILE="../../wikipedia.zip"
DEST_DIR="../../input"

# Check if destination directory exists and contains files
if [ -d "$DEST_DIR/wikipedia" ] && [ -n "$(ls -A "$DEST_DIR/wikipedia" 2>/dev/null)" ]; then
    echo "Destination directory already exists and contains files: $(realpath "$DEST_DIR/wikipedia")"
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
dest_dir = Path("$DEST_DIR")

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

exit 0