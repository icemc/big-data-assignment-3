#!/bin/bash

CLASSES=("RunLSA" "RunLSASimpleTokenizer")
NUM_TERMS=("5000" "10000" "20000")
K_VALUES=("25" "100" "250")

# Generate commands for each class
for CLASS in "${CLASSES[@]}"; do
  # Create a separate file for each class
  OUTPUT_FILE="${CLASS}_commands.sh"
  echo "#!/bin/bash" > "$OUTPUT_FILE"
  echo "# Spark submit commands for $CLASS" >> "$OUTPUT_FILE"
  echo "" >> "$OUTPUT_FILE"
  
  # Generate all combinations for this class
  for TERMS in "${NUM_TERMS[@]}"; do
    for K in "${K_VALUES[@]}"; do
      echo "sbatch launch-spark-submit ../output/RunLSA.jar $CLASS 1.0 $TERMS $K" >> "$OUTPUT_FILE"
    done
  done
  
  # Make the file executable
  chmod +x "$OUTPUT_FILE"
  
  echo "Generated commands for $CLASS in $OUTPUT_FILE"
done

echo "All command files generated"