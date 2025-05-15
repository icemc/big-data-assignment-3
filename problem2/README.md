# NOAA Weather Prediction Analysis

Predict Luxembourg air temperatures using historical NOAA weather data with Spark-based machine learning models (Linear Regression and Random Forest).

## 📋 Project Objective
Build predictive models for Luxembourg temperatures using:
- 77 years of NOAA station data (1949-2025)
- Spark ML for distributed processing

**Key Features**:
- Automated data pipeline (download → process → analyze)
- Comparative analysis of Linear Regression vs Random Forest
- Cluster-ready implementation for HPC systems

## ⚙️ Requirements
- **Java**: 8+ (Recommended: JDK 17)
- **Spark**: 3.5.4+
- **Python**: 3.8+ (Optional for visualization)
- **Storage**: ~5GB disk space for raw data
- **Memory**: 16GB+ recommended

## File Structure
```text
root/
│── input/
│   └── NOAA-065900/                # Processed data (auto-created)
└── problem2/
    ├── src/                        # Scala source code
    ├── scripts/
    │   ├── report.txt              # Results summary (generated when using run.sh)
    │   ├── build.sh                # Compilation script
    │   ├── run.sh                  # Analysis execution
    │   ├── download_noaa_data.sh   # Data downloader
    │   ├── submit_local.sh         # Local pipeline runner
    │   └── submit_job.sh           # HPC job submission
    └── output/
        ├── report.txt              # Results summary (if running from output directory)
        └── WeatherPrediction.jar   # Compiled JAR
```

## 🚀 Quick Start

Navigate to scripts directory and run

```bash
chmod +x *.sh
```


### Option 1: Local Execution

#### 1. Download data
```bash
./download_noaa_data.sh
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

| Script                  | Purpose                                   |
|-------------------------|-------------------------------------------|
| `download_noaa_data.sh` | Downloads and extracts NOAA station data  |
| `build.sh`              | Compiles project to JAR                   |
| `run.sh`                | Executes Spark analysis                   |
| `submit_local.sh`       | Local run wrapper (download+build+run)    |
| `submit_job.sbatch`     | HPC job submission script                 |

## 📊 Sample Results

Result analysis will be saved in `scripts/report.txt` file after job has completed.

```text
=== Weather Prediction Analysis Report ===
Generated: 2025-04-24T23:44:48.412

1. DATA SUMMARY:
----------------
- Total records processed: 1169099
- Training period: 1949-2023 (1136215 records)
- Validation year: 2024 (25208 records)
- Test year: 2025 (7676 records)

2. MODEL PERFORMANCE:
--------------------
Linear Regression:
- Validation RMSE (2024): 6.725581015653365
- Test RMSE (2025): 6.6303073685074105
- Training time: 4.49 seconds

Random Forest:
- Validation RMSE (2024): 2.7608975405216385
- Test RMSE (2025): 2.735520108777691
- Training time: 5.72 seconds

3. PERFORMANCE COMPARISON:
-------------------------
Random Forest was slower to train
but achieved better accuracy

4. FEATURE ANALYSIS:
-------------------
Feature Importances (Random Forest):
  - dew_point       0.6047
  - month           0.2693
  - ceiling_height  0.0566
  - visibility      0.0337
  - hour            0.0274
  - wind_direction  0.0056
  - year            0.0012
  - wind_speed      0.0008
  - day             0.0006
  - elevation       0.0001
  - latitude        0.0000
  - longitude       0.0000

Temperature Correlations:
  - ceiling_height  0.2332
  - month           0.1772
  - dew_point       0.1241
  - hour            0.1098
  - year            0.1016
  - elevation       -0.0834
  - visibility      0.0814
  - wind_direction  0.0486
  - latitude        0.0302
  - longitude       -0.0129
  - day             0.0119
  - wind_speed      -0.0033

Strongest Correlation: ceiling_height (0.23317322861487194)

5. OBSERVATIONS:
---------------
The Random Forest model outperformed Linear Regression

Key Findings:
1. The most predictive feature was dew_point
2. Temperature shows strongest correlation with ceiling_height
3. Model performance difference: 3.8947872597297195 RMSE

6. SAMPLE PREDICTIONS:
---------------------
First 10 test predictions (Linear Regression):
----------------------------------------------
1/1/2025 0:00 | Actual: -0.9°C | Predicted: 9.8°C
1/1/2025 0:00 | Actual: -1.0°C | Predicted: 6.0°C
1/1/2025 0:00 | Actual: -1.0°C | Predicted: 9.8°C
1/1/2025 1:00 | Actual: -0.5°C | Predicted: 9.9°C
1/1/2025 1:00 | Actual: -1.0°C | Predicted: 9.8°C
1/1/2025 1:00 | Actual: 0.0°C | Predicted: 9.9°C
1/1/2025 2:00 | Actual: -0.5°C | Predicted: 10.0°C
1/1/2025 2:00 | Actual: 0.0°C | Predicted: 9.9°C
1/1/2025 2:00 | Actual: 0.0°C | Predicted: 6.2°C
1/1/2025 3:00 | Actual: -0.4°C | Predicted: 10.1°C


First 10 test predictions (Random Forest):
1/1/2025 0:00 | Actual: -0.9°C | Predicted: -0.2°C
1/1/2025 0:00 | Actual: -1.0°C | Predicted: -0.3°C
1/1/2025 0:00 | Actual: -1.0°C | Predicted: -0.2°C
1/1/2025 1:00 | Actual: -0.5°C | Predicted: -0.2°C
1/1/2025 1:00 | Actual: -1.0°C | Predicted: -0.2°C
1/1/2025 1:00 | Actual: 0.0°C | Predicted: 0.7°C
1/1/2025 2:00 | Actual: -0.5°C | Predicted: -0.2°C
1/1/2025 2:00 | Actual: 0.0°C | Predicted: 0.7°C
1/1/2025 2:00 | Actual: 0.0°C | Predicted: -0.2°C
1/1/2025 3:00 | Actual: -0.4°C | Predicted: -0.2°C
```