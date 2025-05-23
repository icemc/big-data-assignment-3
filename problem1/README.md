# Latent Semantic Analysis (LSA) with Spark and Stanford NLP

This project performs Latent Semantic Analysis (LSA) using Apache Spark to analyze a large corpus of Wikipedia articles. It supports two distinct approaches for text preprocessing:

## 🧠 Approaches

1. NLP-Based Tokenization with Apache OpenNLP

Uses OpenNLP for sentence detection and tokenization, and applies lemmatization for cleaner text analysis. This method is more linguistically informed and works better for nuanced semantic extraction.

- Advantages
    - Handles punctuation, sentence boundaries, and word variations better

    - More accurate token extraction for English text

2. Regex-Based Tokenization (Simple Tokenizer)

This version uses basic regular expressions to tokenize and filter words, suitable for performance-critical scenarios or when linguistic precision is less important.

- Tokenizer: Regex-based filtering with patterns for words and numbers

- Regex patterns:

    - Words: ^[a-z_\\-]{6,24}$

    - Numbers: ^-?[0-9]+([.,][0-9]+)?$

- Advantages:

    - Faster processing with minimal dependencies

    - Easier to adapt for non-English text or domain-specific formats

---

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
│   ├── wikipedia/articles                  # input data (auto-created)
│   └── wikipedia/stopwords.txt             # input data (auto-created)
└── problem1/
    ├── src/                        # Scala source code
    ├── scripts/
    │   ├── build.sh                # Compilation script
    │   ├── run.sh                  # Analysis execution
    │   ├── download_data.sh        # Data downloader
    │   ├── submit_local.sh         # Local pipeline runner
    │   └── submit_job.sh           # HPC job submission
    ├── output/
    │    └── RunLSA.jar             # Compiled JAR
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
./download_data.sh
```

#### 2. Build
```bash
./build.sh
```

#### 3. Run analysis (This will run NLP version: RunLSA 0.01 5000 25 )
```bash
./run.sh
```

#### 4. Run with parameters

`run.sh` accepts parameters run `./run.sh --help` to view the various parameters

```bash
./run.sh  RunLSA 0.01 5000 25
```

### Option 2: HPC Execution

- Navigate to the scripts directory

```bash
cd scripts
```

- Elevate permissions

```bash
chmod +x *.sh
```

#### 1. Download required data

```bash 
./download_data.sh
```

#### 2. Setup scala using EasyBuild

- Create an interactive session by running the following on the HPC command line

    ```bash
    salloc -p interactive --qos debug --time=2:00:00 -N 1 -n 1 -c 32
    ```

- Load EasyBuild module 

    ```bash
    module load tools/EasyBuild/5.0.0
    ```

- Get your easybuild source path 

    ```bash
    eb --show-config | grep sourcepath
    ```
    You should get something similar to this

    `sourcepath /home/users/ltemgoua/.local/easybuild/sources`

    `/home/users/ltemgoua/.local/easybuild/sources` is your easybuild source path. You will need this for the next step.


- Download the scala binary to the easybuild source path (replace `/path/to/easybuild/sources/` with the value you obtained from the previous step)

    ```bash
    wget https://github.com/scala/scala/releases/download/v2.12.15/scala-2.12.15.tgz -P /path/to/easybuild/sources/
    ```

- Build and install 

    ```bash
    eb scala-2.12.15.eb --robot
    ```

- Load the scala module

    ```bash
    module load lang/Scala/2.12.15
    ```

Now you are all set to run the code on the HPC.

#### 3. Build the fat Jar

```bash
./build.sh
```

#### 4. Run analysis (This will run NLP version: RunLSA 1.0 5000 25 )
```bash
./submit_job.sh
```

#### 5. Run with parameters

`submit_job.sh` accepts parameters run `./submit_job.sh --help` to view the various parameters

```bash
./submit_job.sh  RunLSA 1.0 20000 250
```

### Running the Search Engine

After running the analysis with different configuration and Models (RunLSA or RunLSASimpleTokenizer) each model will contain a directory with its name and sub directories containing the data for the different configurations ran.

```text
scripts/
│── RunLSA/                             # For all RunLSA (LSA with NLP using Stanford library)
│   ├── model_terms5000_k25             # for RunLSA 1.0 5000 25
│   ├── model_terms20000_k250           # for RunLSA 1.0 20000 250
│
└── RunLSASimpleTokenizer/              # For all RunLSASimpleTokenizer (LSA with with regex)
    ├── model_terms5000_k25             # for RunLSASimpleTokenizer 1.0 5000 25
    ├── model_terms20000_k250           # for RunLSASimpleTokenizer 1.0 20000 250


```



| Script             | Purpose                                   |
|--------------------|-------------------------------------------|
| `download_data.sh` | Downloads and extracts data               |
| `build.sh`         | Compiles project to JAR                   |
| `run.sh`           | Executes Spark analysis                   |
| `submit_job.sh`    | HPC job submission script                 |

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