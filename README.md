# Olist Big Data Pipeline

## 1. Overview

PySpark pipeline to process Brazilian e-commerce data from Olist.
Raw CSV files are transformed into analytical tables organized in a data lake architecture.


## 2. Architecture

```
├── data                # All files (csv and parquet)
│   ├── 0_raw           # Original CSV files, unmodified
│   ├── 1_bronze        # Raw data converted to Parquet
│   ├── 2_silver        # Cleaned and typed data
│   └── 3_gold          # Data ready for analysis and KPIs
├── notebooks               # Notebooks detailling data treatment
│   ├── 1_bronze.ipynb      # Conversion csv to parquet
│   ├── 2_silver.ipynb      # Data typing and cleaning
│   └── 3_gold.ipynb        # Data preparation for analysis
├── pipeline.py         # Full pipeline from raw to gold
├── pyproject.toml
├── README.md
└── uv.lock
```


## 3. Dataset

Brazilian E-Commerce Public Dataset by Olist ([Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce))

| Table | Rows | Description |
|-------|------|-------------|
| orders | 99 441 | Main orders table |
| customers | 99 441 | Customer information |
| order_items | 112 650 | Items per order |
| payments | 103 886 | Payment details |
| products | 32 951 | Product catalog |
| sellers | 3 095 | Seller information |
| reviews | 104 162 | Customer reviews |
| geolocation | 1 000 163 | GPS coordinates by zip code |
| translation | 71 | Product category name translations |

## 4. Business KPIs

| KPI | Value |
|-----|-------|
| Total revenue | 15 865 616 BRL |
| Average cart | 160.56 BRL |
| Average delivery delay | 12.5 days |
| Lateness rate | 7.99% |
| Average review score | 4.15 / 5 |
| Top category | Health & Beauty (9.26%) |
| Top payment method | Credit card (73.97%) |


## 5. Setup

### A. Requirements
- Python >= 3.11
- Java 17
- uv
- download [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset and place the 9 csv files in the `data/0_raw` folder

### B. Java Configuration
**Linux (Ubuntu / Debian)**
```bash
sudo apt install openjdk-17-jdk
uv sync
```

**MacOS**
```bash
brew install openjdk@17
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> ~/.zshrc
source ~/.zshrc
uv sync
```
If PySpark cannot find Java, uncomment this line at the top of each script:

```python
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
```

### C. Run script or notebooks

**Script**
```bash
uv run python pipeline.py
```

**Notebooks**  
  
Select kernel (at the top right in VSCode) >> bigdata >> run all cells


## 6. Contributors

- **Sarah Azzi** - [Github](https://https://github.com/SarahAzzI)
- **Flora Trecul** - [Github](https://github.com/Flora-Trecul)
- **Ethan Puype** - [Github](https://github.com/NICHIKU)