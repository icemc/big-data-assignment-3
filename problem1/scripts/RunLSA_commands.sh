#!/bin/bash
# Spark submit commands for RunLSA

sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 5000 25
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 5000 100
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 5000 250
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 10000 25
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 10000 100
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 10000 250
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 20000 25
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 20000 100
sbatch launch-spark-submit ../output/RunLSA.jar RunLSA 1.0 20000 250
