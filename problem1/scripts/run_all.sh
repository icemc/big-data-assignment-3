#!/bin/bash

# Configuration
SCRIPT_DIR=$(dirname "$0")
RUN_SCRIPT="${SCRIPT_DIR}/run.sh"

# Hyperparameters to test
CLASSES=("RunLSA" "RunLSASimpleTokenizer")
SAMPLE_SIZES=("0.02")
NUM_TERMS=("5000" "10000" "20000")
K_VALUES=("25" "100" "250")

# Function to run a single experiment
run_experiment() {
  local class=$1
  local sample_size=$2
  local num_terms=$3
  local k=$4

  # Create a unique identifier for this run
  local run_id="${class}_sample${sample_size}_terms${num_terms}_k${k}"

  echo "===================================================================="
  echo "Starting experiment: $run_id"
  echo "===================================================================="
  echo "Parameters:"
  echo "  Class: $class"
  echo "  Sample size: $sample_size"
  echo "  Num terms: $num_terms"
  echo "  k: $k"
  echo "----------------------------------------"

  # Record start time
  local start_time=$(date +%s)

  # Run the experiment and capture output
  $RUN_SCRIPT "$class" "$sample_size" "$num_terms" "$k"

  # Calculate runtime
  local end_time=$(date +%s)
  local runtime=$((end_time - start_time))

  # Check exit status
  if [ $? -eq 0 ]; then
    echo "----------------------------------------"
    echo "Experiment $run_id completed successfully in $runtime seconds"
  else
    echo "----------------------------------------"
    echo "Experiment $run_id failed after $runtime seconds"
  fi

  echo ""
}

# Main execution loop
for class in "${CLASSES[@]}"; do
  for sample_size in "${SAMPLE_SIZES[@]}"; do
    for num_term in "${NUM_TERMS[@]}"; do
      for k in "${K_VALUES[@]}"; do
        run_experiment "$class" "$sample_size" "$num_term" "$k"

        # Add a small delay between runs if needed
        sleep 2
      done
    done
  done
done

echo "All experiments completed."