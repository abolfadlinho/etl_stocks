import os
import pandas as pd
import logging
import time
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import argparse

# === Load Config === #
load_dotenv()

CSV_PATH = os.getenv("CSV_PATH")
DATABASE_URI = os.getenv("DATABASE_URI")
DEFAULT_CUTOFF_DATE = os.getenv("DEFAULT_CUTOFF_DATE")

# === Logger Setup === #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)

# === ETL Functions === #
def extract(csv_path: str, cutoff_date: str) -> pd.DataFrame:
    """
    Extracts data from CSV file and filters rows before the cutoff date.
    """
    try:
        df = pd.read_csv(csv_path, parse_dates=["Date"])
        logging.info(f"CSV loaded successfully with {len(df)} rows.")
        df = df[df["Date"] < pd.to_datetime(cutoff_date)]
        logging.info(f"Filtered data to {len(df)} rows before {cutoff_date}.")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms column names and validates data.
    """
    try:
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Optional: Drop rows with nulls in critical columns
        df.dropna(subset=["symbol", "date", "close"], inplace=True)

        # Optional: Deduplicate
        df.drop_duplicates(subset=["symbol", "date"], inplace=True)

        logging.info("Transformation complete.")
        return df
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise

def load(df: pd.DataFrame, db_uri: str, table_name: str = "stocks"):
    """
    Loads the DataFrame into a PostgreSQL table.
    """
    try:
        engine = create_engine(db_uri)
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="append", index=False)
        logging.info(f"Loaded {len(df)} rows into '{table_name}' table.")
    except Exception as e:
        logging.error(f"Load failed: {e}")
        raise

def run_etl(cutoff_date: str):
    """
    Orchestrates the ETL process.
    """
    logging.info("==== ETL PIPELINE STARTED ====")
    start_time = time.time()

    try:
        df_extracted = extract(CSV_PATH, cutoff_date)
        df_transformed = transform(df_extracted)
        load(df_transformed, DATABASE_URI)
    except Exception as e:
        logging.critical("ETL process failed. See logs for details.")
    finally:
        elapsed = time.time() - start_time
        logging.info(f"==== ETL PIPELINE FINISHED in {elapsed:.2f}s ====")

# === Main === #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline for S&P 500 data.")
    parser.add_argument("--cutoff", type=str, default=DEFAULT_CUTOFF_DATE,
                        help="Cutoff date (YYYY-MM-DD) to simulate extraction before a given date.")
    args = parser.parse_args()

    run_etl(args.cutoff)
