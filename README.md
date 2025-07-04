# 🛠️ S&P 500 Historical Stock Data ETL Pipeline

A production-ready, modular **ETL pipeline** built with **Python**, **PostgreSQL**, and **SQLAlchemy**, designed to ingest, transform, and load historical stock price data from the **S&P 500 index** into a **relational data warehouse**. This project simulates a **time-aware batch data ingestion** use case using a configurable **cutoff date**.

> 🚀 Built for **resume impact** and designed with **Data Engineering best practices** in mind.

---

## 📌 Features

✅ **ETL Architecture**: Fully modular Extract, Transform, Load pipeline  
✅ **Environment Configuration**: Uses `.env` for secure and scalable config management  
✅ **Logging**: Dual logging to file and console for full audit traceability  
✅ **CLI Support**: Dynamic `cutoff` parameter via `argparse`  
✅ **PostgreSQL Data Warehouse Integration**  
✅ **Schema Enforcement** and **Data Validation**  
✅ **Deduplication Logic** for idempotent loads  
✅ **Time-Based Filtering** for historical backfilling  
✅ **Production-Grade Code Quality**  
✅ **Buzzword-Ready for Recruiters™**

---

---

## 📊 Dataset

- **Source**: [Kaggle - S&P 500 Historical Data](https://www.kaggle.com/)
- **Format**: CSV
- **Date Range**: `2014-12-22` to `2024-12-20`
- **Columns**: `Date`, `Symbol`, `Open`, `High`, `Low`, `Close`, `Adj Close`, `Volume`

---

## 🧱 Tech Stack

| Layer         | Technology       |
| ------------- | ---------------- |
| Language      | Python 3.9+      |
| Storage Layer | PostgreSQL       |
| Interface     | SQLAlchemy       |
| Scripting     | Pandas, dotenv   |
| Configuration | .env files       |
| Logging       | Python logging   |
| Orchestration | CLI / Cron-ready |

---

## 🧰 Installation

### 1. Clone the repo

```bash
git clone https://github.com/your-username/sp500-etl-pipeline.git
cd sp500-etl-pipeline
```

### 2. Create a virtual environment and install dependencies

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the root directory:

```env
CSV_PATH=sp500_stocks.csv
DATABASE_URI=postgresql://postgres:your_password@localhost:5432/stock_data
DEFAULT_CUTOFF_DATE=2020-01-01
```

> Make sure your PostgreSQL database is created and accessible.

---

## 🚀 Usage

```bash
python etl_pipeline.py --cutoff 2019-01-01
```

This will:

- Extract all data **before the given date**
- Transform and validate column names
- Remove nulls and duplicate records
- Load the clean data into your **PostgreSQL warehouse**

---

## 💼 Real-World Concepts Demonstrated

- 🔄 **Batch Processing**
- 🏗️ **Data Warehousing**
- 🧼 **Data Cleaning & Standardization**
- 💾 **Data Ingestion**
- 🔒 **Secure Credential Handling**
- 📋 **Schema Mapping**
- 🔁 **Idempotent Loads**
- ⚙️ **Automation Readiness** with `cron` or Airflow
- 📈 **Time Series Aware ETL**

---

## 🧪 Sample SQL Query

```sql
SELECT symbol, AVG(close) AS avg_close
FROM stocks
WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY symbol
ORDER BY avg_close DESC;
```

---

## 📅 Automating the Pipeline (Optional)

Use `cron` (Linux/macOS) or **Task Scheduler** (Windows) for automated daily runs.

Example cron job:

```cron
0 6 * * * /usr/bin/python3 /path/to/etl_pipeline.py --cutoff $(date +\%Y-\%m-\%d)
```

---

## ✨ Future Enhancements

- 🛠 Integrate **Apache Airflow** or **Dagster** for orchestration
- ⛓ Add **unit tests** with `pytest`
- 🐘 Use **partitioning** and **indexes** in PostgreSQL
- 🧩 Add **data quality checks** and **monitoring**
- 📦 Export transformed data to **Parquet** for lakehouse compatibility
- ☁️ Migrate to **AWS RDS** or **Google Cloud SQL**

---
