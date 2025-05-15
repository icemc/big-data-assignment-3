# 💓 Heart Disease Prediction using Apache Spark

This project implements a machine learning pipeline in Apache Spark (Scala) to analyze health survey data and predict:

- Whether a person has **heart disease** (binary classification).
- The **age category** of a person (multi-class classification).

It uses **Decision Tree** and **Random Forest** classifiers, incorporating hyperparameter tuning with both `CrossValidator` and `TrainValidationSplit`.

---

## 📂 Dataset

The dataset used is the **Heart Disease UCI** dataset, assumed to be preprocessed and stored as a CSV file (`heart_2020_cleaned.csv`). It includes a mix of categorical and numerical features such as:

- `BMI`, `Smoking`, `AlcoholDrinking`, `PhysicalHealth`, `MentalHealth`, `DiffWalking`, etc.
- `HeartDisease` is the main target variable in parts (b) and (c).
- `AgeCategory` is used as the target in part (d).

---

## 🧠 Machine Learning Tasks

### ✅ Binary Classification: Heart Disease
- **Algorithms**: Decision Tree with CrossValidator and TrainValidationSplit
- **Metrics**:
    - Accuracy
    - Area Under ROC
    - Area Under PR

### 🎯 Multi-class Classification: Age Category
- **Algorithm**: Random Forest
- **Metrics**:
    - Accuracy
    - Weighted Precision
    - Weighted Recall
    - F1 Score

---

## 🔧 Project Structure

### Main Components
| Part | Task | Description |
|------|------|-------------|
| (a) | Data Loading & Exploration | Reads the CSV data, defines schema, prints dataset stats |
| (b) | Decision Tree + CrossValidator | Binary classification of heart disease |
| (c) | Decision Tree + TrainValidationSplit | Alternative validation technique |
| (d) | Random Forest + TrainValidationSplit | Predicts age category using multiple features |

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
│   └── Heart-Disease/              # Processed data (auto-created)
└── problem1/
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
    │    └── WeatherPrediction.jar   # Compiled JAR
    │    
    ├── README.md
    └── problem1.txt
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

#### 1. Run all stages by submitting an sbatch script
```bash
./submit_job.sbatch
```

| Script             | Purpose                                   |
|--------------------|-------------------------------------------|
| `download_data.sh` | Downloads and extracts NOAA station data  |
| `build.sh`         | Compiles project to JAR                   |
| `run.sh`           | Executes Spark analysis                   |
| `submit_local.sh`  | Local run wrapper (download+build+run)    |
| `submit_job.sbatch` | HPC job submission script                 |

## 📊 Sample Results

Result analysis will be saved in `scripts/report.txt` file after job has completed.

```text
=== Problem 1 Results ===

== Part (b): Decision Tree with CrossValidator ==
Best Parameters:
- Impurity: entropy
- Max Depth: 24
- Max Bins: 100

Test Metrics:
- Area under ROC: 0.4636
- Area under PR: 0.0777
- Accuracy: 0.8847

Execution Time: 5m 3s

== Part (c): Decision Tree with TrainValidationSplit ==
Best Parameters:
- Impurity: entropy
- Max Depth: 24
- Max Bins: 100

Test Metrics:
- Area under ROC: 0.4636
- Area under PR: 0.0777
- Accuracy: 0.8847

Execution Time: 1m 14s

== Part (d): Random Forest with AgeCategory Target ==
Best Parameters:
- Impurity: gini
- Max Depth: 12
- Max Bins: 100
- Num Trees: 20

Test Metrics:
- Accuracy: 0.1739
- Weighted Precision: 0.1622
- Weighted Recall: 0.1739
- F1: 0.1416

Execution Time: 33m 24s

=== Total Execution Time ===
39m 50s
```