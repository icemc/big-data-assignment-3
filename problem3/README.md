# Recommender Systems via Matrix Factorization

This project implements a music recommender system using Apache Spark's MLlib library, specifically leveraging the Alternating Least Squares (ALS) algorithm for collaborative filtering.

## Features

- **Data Processing**: Efficiently processes artist, user, and play count data
- **Model Training**: Implements ALS for implicit feedback (play counts)
- **Evaluation**:
    - AUC (Area Under the ROC Curve) metrics
    - Precision, Recall, and Accuracy calculations
    - Comparison against a popularity baseline
- **Hyperparameter Tuning**: Optimizes rank, lambda, and alpha parameters
- **New User Recommendations**: Demonstrates how to generate recommendations for a new user

## ⚙️ Requirements
- **Java**: 8+ (Recommended: JDK 17)
- **Scala**: 2.12
- **Spark**: 3.5.4+
- **Python**: 3.8+ (Optional for visualization)
- **Storage**: ~5GB disk space for raw data
- **Memory**: 16GB+ recommended

## Key Metrics

The system reports:
- AUC scores for both the recommender and baseline models
- Precision, recall, and accuracy of the final model
- Top recommendations for a new user profile


## Note

This implementation includes optimizations for:
- Efficient data caching
- Broadcast variables for shared data
- Memory management
- Serialization improvements


## File Structure
```text
root/
│── input/
│   └── audioscrobbler/             # Input data set (auto-created)
└── problem3/
    ├── src/                        # Scala source code
    ├── scripts/
    │   ├── report.txt              # Results summary (generated when using run.sh)
    │   ├── build.sh                # Compilation script
    │   ├── run.sh                  # Analysis execution
    │   ├── download_data.sh        # Data downloader
    │   ├── submit_local.sh         # Local pipeline runner
    │   └── submit_job.sbatch       # HPC job submission
    ├── output/
    │   ├── report.txt              # Results summary (if running from output directory)
    │   └── WeatherPrediction.jar   # Compiled JAR
    ├── README.md
    └── problem3.txt
```

## 🚀 Quick Start

Navigate to scripts directory and run

```bash
chmod +x *.sh
```

### Option 1: Local Execution

#### 1. download data
```bash
./unzip_data.sh
```

#### 2. Build
```bash
./build.sh
```

#### 3. Run analysis
```bash
./run.sh
```

#### OR to run all 3 stages use

##### 4. Run all stages
```bash
./submit_local.sh
```

### Option 2: HPC Execution

#### 1. Run all stages by submitting a bash script
```bash
./submit_job.sh
```

| Script            | Purpose                             |
|-------------------|-------------------------------------|
| `download_data.sh`        | Unzip and extracts input data       |
| `build.sh`        | Compiles project to JAR             |
| `run.sh`          | Executes Spark analysis             |
| `submit_local.sh` | Local run wrapper (unzip+build+run) |
| `submit_job.sh`   | HPC job submission script           |


## 📊 Sample Results

Result analysis will be saved in `scripts/report.txt` file after job has completed.

```text
=== Recommender System Analysis Report ===
Generated: 2025-04-24T23:18:18.922

1. DATA SUMMARY:
----------------
- Artist data records: 1848281
- Artist alias records: 190892
- User-artist interaction records: 24296858
- Users with ≥100 artists: 72748

2. DATA SPLITS:
--------------
- Training data size: 17041680
- Test data size: 4263157
- Cross-validation subset: 5106789 records
- Evaluation users: 500

3. MODEL PERFORMANCE:
--------------------
Base Model:
- AUC: 0.6311386464250404
- Training parameters: rank=10, lambda=0.01, alpha=1.0

Baseline (Popularity-based):
- AUC: 0.5420199861195063

Optimized Model:
- Best parameters: rank=50, lambda=0.01, alpha=1.0
- Final AUC: 0.6481788435956171
- Precision: 0.56584
- Recall: 0.12765073882915987
- Accuracy: 0.56584

4. HYPERPARAMETER TUNING:
------------------------
Tested configurations:
- Ranks: 10, 25, 50
- Lambdas: 1.0, 0.1, 0.01
- Alphas: 1.0, 10.0, 100.0

Best configuration achieved 66.5% of maximum possible AUC

5. NEW USER RECOMMENDATIONS:
---------------------------
Created new user ID: 2443549
User's favorite artists:
  - Portishead                     (ID: 1, plays: 100)
- Phil Collins Big Band          (ID: 1000006, plays: 50)
- The Phil Collins Big Band      (ID: 1000007, plays: 75)
- A Perfect Circle               (ID: 1000009, plays: 120)
- Aerosmith                      (ID: 1000010, plays: 200)
- MC Hawking                     (ID: 1000013, plays: 30)
- Pantera                        (ID: 1000014, plays: 45)
- 06Crazy Life                   (ID: 1134999, plays: 150)
- Pang Nakarin                   (ID: 6821360, plays: 90)
- Bodenstandig 3000              (ID: 6826647, plays: 60)

Top 10 recommendations:
  - Metallica                      (ID: 1000024, score: 0.9562)
- Nirvana                        (ID: 976, score: 0.9356)
- Pink Floyd                     (ID: 82, score: 0.9121)
- System of a Down               (ID: 4468, score: 0.8948)
- Rage Against the Machine       (ID: 1014421, score: 0.8940)
- Red Hot Chili Peppers          (ID: 1274, score: 0.8932)
- Led Zeppelin                   (ID: 1394, score: 0.8879)
- Guns N' Roses                  (ID: 1000323, score: 0.8612)
- Marilyn Manson                 (ID: 4061, score: 0.8581)
- Nine Inch Nails                (ID: 831, score: 0.8474)

6. KEY OBSERVATIONS:
-------------------
1. The optimized model outperformed the baseline by 10.6% AUC
2. Best performance achieved with rank 50 and alpha 1.0
3. Precision/Recall metrics suggest moderate recommendation quality
4. New user recommendations show moderate predicted affinity

7. RESOURCE UTILIZATION:
-----------------------
- Memory optimization: Cached key datasets (trainData, allRatings)
- Parallelism: 8 partitions for shuffle operations
- Broadcast variables used for artist/alias data
```