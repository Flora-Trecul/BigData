# Olist Big Data Pipeline

## Overview
PySpark pipeline to process Brazilian e-commerce data from Olist.
Raw CSV files are transformed into analytical tables organized in a data lake architecture.

## Data Lake Architecture

| Zone | Folder | Description |
|------|--------|-------------|
| Raw | `data/0_raw/` | Original CSV files, unmodified |
| Bronze | `data/1_bronze/` | Raw data converted to Parquet |
| Silver | `data/2_silver/` | Cleaned and typed data |
| Gold | `data/3_gold/` | Business indicators ready for analysis |

## Dataset
Brazilian E-Commerce Public Dataset by Olist
[Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Tables

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

## Business KPIs

| KPI | Value |
|-----|-------|
| Total revenue | 15 865 616 BRL |
| Average cart | 160.56 BRL |
| Average delivery delay | 12.5 days |
| Lateness rate | 7.99% |
| Average review score | 4.15 / 5 |
| Top category | Health & Beauty (9.26%) |
| Top payment method | Credit card (73.97%) |

## Data Quality Issues

| Table | Issue | Action |
|-------|-------|--------|
| reviews | Corrupted rows due to newlines in comments | Fixed with `multiLine=True` |
| reviews | 85 duplicates | Removed with `dropDuplicates()` |
| reviews | 2 236 rows without order_id | Removed |
| orders | 14 delivered orders without approval date | Removed |
| payments | 9 negative payment values | Removed |
| payments | 3 rows with payment_type = not_defined | Removed |

## Setup

### Requirements
- Python >= 3.11
- Java 17
- uv

### Installation
```bash
brew install openjdk@17
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> ~/.zshrc
source ~/.zshrc
uv sync
```
### Important for macOS users
If PySpark cannot find Java, add this line at the top of each script before the imports:

```python
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
```

### Run

**Ingestion (Raw → Bronze)**
```bash
uv run python notebooks/ingestion.py
```

**Silver (Bronze → Silver)**
```bash
uv run python notebooks/silver.py
```

**Gold (Silver → Gold)**
Open `notebooks/3_gold.ipynb` in VS Code and run all cells.

## Project Structure