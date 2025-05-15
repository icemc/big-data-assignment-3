#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# PHASE 1: Unzip the required Data
echo "=== PHASE 1: Download the required Data ==="
cd "$SCRIPT_DIR" || exit 1

if [ -f "./download_data.sh" ]; then
    echo "Starting unzip..."
    start_time=$SECONDS

    ./download_data.sh

    duration=$((SECONDS - start_time))
    echo "Download completed in $duration seconds"
else
    echo "Error: download_data.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Phase 2: Build
echo -e "\n=== PHASE 2: Build Phase ==="
cd "$SCRIPT_DIR" || exit 1

if [ -f "./build.sh" ]; then
    echo "Starting build..."
    start_time=$SECONDS

    ./build.sh
    BUILD_EXIT=$?

    duration=$((SECONDS - start_time))
    echo "Build completed in $duration seconds"

    if [ $BUILD_EXIT -ne 0 ]; then
        echo "Error: Build failed with exit code $BUILD_EXIT"
        exit $BUILD_EXIT
    fi
else
    echo "Error: build.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Phase 3: Run
echo -e "\n=== PHASE 3: Run Phase ==="
cd "$SCRIPT_DIR" || exit 1

if [ -f "./run.sh" ]; then
    echo "Starting analysis..."
    start_time=$SECONDS

    ./run.sh
    RUN_EXIT=$?

    duration=$((SECONDS - start_time))
    echo "Analysis completed in $duration seconds"

    if [ $RUN_EXIT -ne 0 ]; then
        echo "Error: Run failed with exit code $RUN_EXIT"
        exit $RUN_EXIT
    fi
else
    echo "Error: run.sh not found in $SCRIPT_DIR"
    exit 1
fi

echo -e "\n=== Job Completed Successfully ==="
echo "Output available in:"
echo "- Results: report.txt"
exit 0