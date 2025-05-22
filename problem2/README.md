# Wikipedia Movie Plots - Latent Semantic Analysis (LSA)

This Scala Spark project performs Latent Semantic Analysis (LSA) on the Wikipedia Movie Plots dataset using Stanford CoreNLP and Apache Spark's distributed capabilities. It supports keyword-based searches that return semantically relevant movie plots along with their genres.


**Key Features**:
- Automated data pipeline (download → process → analyze)
- Builds a term-document matrix with TF-IDF scores.
- Performs Singular Value Decomposition (SVD) to extract latent semantic structures.
- Returns most relevant movie plots for a given keyword query.
- Displays most relevant terms and documents per latent concept.
- Cluster-ready implementation for HPC systems

## ⚙️ Requirements
- **Java**: 8+ (Recommended: JDK 17)
- **Spark**: 3.5.4+
- **Storage**: ~5GB disk space for raw data
- **Memory**: 16GB+ recommended

## File Structure
```text
root/
│── input/
│   └── movie_plots/                    # Processed data (auto-created after running download_data.sh)
└── problem2/
    ├── src/                            # Scala source code
    ├── scripts/
    │   ├── build.sh                    # Compilation script
    │   ├── run.sh                      # Analysis execution
    │   ├── download_data.sh            # Data downloader (required jars + dataset)
    │   ├── submit_local.sh             # Local pipeline runner
    │   └── submit_job.sh               # HPC job submission
    └── output/
        └── WikipediaMoviePlotLSA.jar   # Compiled JAR
```

## 🚀 Quick Start

Navigate to scripts directory and run

```bash
cd scripts
```

Then elevate the permissions of all the scripts

```bash
chmod +x *
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

Create an interactive session by running the following on the HPC command line

```bash
salloc -p interactive --qos debug --time=2:00:00 -N 1 -n 1 -c 32
```
Then run the job interactively. This will download all required files including dataset and dependent jars before running.

```bash
./submit_job.sh
```

## Script summary


| Script                  | Purpose                                   |
|-------------------------|-------------------------------------------|
| `download_noaa_data.sh` | Downloads and extracts NOAA station data  |
| `build.sh`              | Compiles project to JAR                   |
| `run.sh`                | Executes Spark analysis                   |
| `submit_local.sh`       | Local run wrapper (download+build+run)    |
| `submit_job.sh`         | HPC job submission script                 |
-----------------------------------------------------------------------

## 📊 Sample Results

Result analysis will be saved in `scripts/result/<timestamp>.log` file after job has completed.

```text
===== Query: robot future =====
Title: Who Goes Next? (Score: 0.0387 , Genre: war)
Title: The Hook (Score: 0.0384 , Genre: war)
Title: Neutral Port (Score: 0.0333 , Genre: war)
Title: To End All Wars (Score: 0.0323 , Genre: war)
Title: The Space Children (Score: 0.0318 , Genre: sci-fi)
Title: Escape to Danger (Score: 0.0315 , Genre: thriller)
Title: General Post (Score: 0.0312 , Genre: drama)
Title: An Inaccurate Memoir (Score: 0.0305 , Genre: drama / action / war)
Title: Rengō Kantai Shirei Chōkan: Yamamoto Isoroku (Score: 0.0296 , Genre: war drama)
Title: Steel Rain (Score: 0.0268 , Genre: unknown)

===== Query: western cowboy =====
Title: Let 'Em Have It (Score: 0.0581 , Genre: crime)
Title: The Man They Couldn't Arrest (Score: 0.0436 , Genre: drama)
Title: Dakota Lil (Score: 0.0405 , Genre: western)
Title: Lal Dupatta Malmal Ka (Score: 0.0403 , Genre: unknown)
Title: Oklahoma Cyclone (Score: 0.0390 , Genre: western)
Title: Border Law (Score: 0.0366 , Genre: western)
Title: Oliver Twist (Score: 0.0363 , Genre: literary drama)
Title: The Man Outside (Score: 0.0357 , Genre: crime)
Title: Outside the Law (Score: 0.0356 , Genre: film noir)
Title: The Fire Raisers (Score: 0.0343 , Genre: drama)
----------------------------------------
Analysis complete
Runtime: 353 seconds
----------------------------------------
```