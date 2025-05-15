#!/bin/bash
#SBATCH --job-name=recommendation_system
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --time=02:00:00
#SBATCH --mem=16G
#SBATCH --output=recommendation_%j.out
#SBATCH --error=recommendation_%j.err

# Load required modules
module load env/release/2023b # To be able to load the Spark module
module load devel/Spark/3.5.4-foss-2023b-Java-17
module load tools/cURL/8.3.0-GCCcore-13.2.0  # Ensure curl is available
module load lang/Python/3.11.5-GCCcore-13.2.0 # Used to unzip data file

# Get the directory where this script is located
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# PHASE 1: Unzip the required Data
echo "=== PHASE 1: Download the required Data ==="
cd "$SCRIPT_DIR" || exit 1

if [ -f "./download_data.sh" ]; then
    echo "Starting download..."
    start_time=$SECONDS

    ./download_data.sh

    duration=$((SECONDS - start_time))
    echo "Download completed in $duration seconds"
else
    echo "Error: download_data.sh not found in $SCRIPT_DIR"
    exit 1
fi


# Phase 2: Run
echo -e "\n=== PHASE 2: Run Phase ==="
cd "$SCRIPT_DIR" || exit 1

if [ -f "./run_shell.sh" ]; then
    echo "Starting analysis..."
    start_time=$SECONDS

    ./run_shell.sh
    RUN_EXIT=$?

    duration=$((SECONDS - start_time))
    echo "Analysis completed in $duration seconds"

    if [ $RUN_EXIT -ne 0 ]; then
        echo "Error: Run failed with exit code $RUN_EXIT"
        exit $RUN_EXIT
    fi
else
    echo "Error: run_shell.sh not found in $SCRIPT_DIR"
    exit 1
fi

echo -e "\n=== Job Completed Successfully ==="
echo "Output available in:"
echo "- Results: report.txt"
echo "- Logs: recommendation_$SLURM_JOB_ID.{out,err}"
exit 0