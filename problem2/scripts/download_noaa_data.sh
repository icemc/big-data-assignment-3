#!/bin/bash

# NOAA Data Download and Extraction Script
# Properly skips if data exists, downloads (without retries), extracts, and cleans up

# Configuration
BASE_URL="https://www1.ncdc.noaa.gov/pub/data/noaa"
STATION_ID="065900"
TEMP_DIR="../../input/Data"
FINAL_DIR="../../input/NOAA-065900"
MIN_EXPECTED_FILES=70  # 1949-2025 = 77 possible files

# Improved skip logic
check_existing_data() {
    if [ -d "$FINAL_DIR" ]; then
        local file_count=$(find "$FINAL_DIR" -maxdepth 1 -type f | wc -l)
        if [ "$file_count" -ge $MIN_EXPECTED_FILES ]; then
            echo "=== Data already exists in $FINAL_DIR ($file_count files) ==="
            echo "Skipping download and extraction"
            exit 0
        else
            echo "=== Found incomplete dataset in $FINAL_DIR ($file_count files) ==="
            echo "Will download missing files"
        fi
    else
        echo "=== No existing data found ==="
    fi
}

# Create directories
create_dirs() {
    mkdir -p "$TEMP_DIR" "$FINAL_DIR" || {
        echo "Error: Failed to create directories"
        exit 1
    }
}

# Download function (no retries)
download_file() {
    local year=$1
    local filename="${STATION_ID}-99999-${year}.gz"
    local target_file="${FINAL_DIR}/${STATION_ID}-99999-${year}"

    # Skip if final file already exists
    if [ -f "$target_file" ]; then
        echo "Skipping $year (already exists)"
        return 0
    fi

    local url="${BASE_URL}/${year}/${filename}"
    local save_path="${TEMP_DIR}/${filename}"

    echo "Downloading $url..."

    if curl -f -s -S -o "$save_path" "$url"; then
        echo "Success: $filename"
        return 0
    else
        echo "Skipping failed download: $filename"
        [ -f "$save_path" ] && rm "$save_path"
        return 1
    fi
}

# Extraction function
extract_files() {
    echo -e "\nExtracting files to $FINAL_DIR..."
    local success_count=0

    for gz_file in "$TEMP_DIR"/*.gz; do
        if [ -f "$gz_file" ]; then
            local base_name=$(basename "$gz_file" .gz)
            local target_file="$FINAL_DIR/$base_name"

            # Skip if already extracted
            if [ -f "$target_file" ]; then
                echo "Skipping existing: $base_name"
                continue
            fi

            echo "Extracting $base_name..."
            if gunzip -c "$gz_file" > "$target_file"; then
                ((success_count++))
                echo "Extracted successfully"
            else
                echo "Failed to extract, removing partial file"
                [ -f "$target_file" ] && rm "$target_file"
            fi
        fi
    done

    echo -e "\nSuccessfully extracted $success_count new files"
}

# Main execution
echo "=== NOAA Data Download Started ==="
check_existing_data
create_dirs

echo "Note: Failed downloads will be skipped without retries"

# Download all years
for year in {1949..2025}; do
    download_file "$year"
done

# Process downloaded files
extract_files

# Cleanup
echo -e "\nCleaning up temporary files..."
rm -rf "$TEMP_DIR" && echo "Temporary directory removed"

echo -e "\n=== Process Completed ==="
echo "Final data available in: $FINAL_DIR"