import os
import logging
import duckdb
import schedule
import time
import pytz
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------- CONFIG ----------------
SILVER_PATH = "az://clean-data/silver/all_bills/data/*/*.parquet"
SILVER_PATH_ITEMS = "az://clean-data/silver/all_items/data/*.parquet"
GOLD_BASE = "az://clean-data/gold"
BLOB_CONN_ENV = "AZURE_BLOB_CONN_STR"

IST = pytz.timezone("Asia/Kolkata")
# ----------------------------------------


# ---------------- DUCKDB INIT ----------------
def initialize_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()

    try:
        con.execute("INSTALL azure;")
        con.execute("LOAD azure;")

        conn_str = os.getenv(BLOB_CONN_ENV)
        if not conn_str:
            raise RuntimeError(f"{BLOB_CONN_ENV} not set")

        con.execute(
            "CREATE OR REPLACE SECRET blob_secret (TYPE azure, CONNECTION_STRING ?);",
            [conn_str]
        )

        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        con.execute("SET azure_transport_option_type='curl';")
        con.execute("PRAGMA threads=4;")

        logger.info("DuckDB initialized successfully with Azure support")
        return con

    except Exception as e:
        logger.error(f"Failed to initialize DuckDB: {e}")
        raise


# ---------------- GENERIC KPI FUNCTION ----------------
def generate_kpi(
    con: duckdb.DuckDBPyConnection,
    query: str,
    output_path: str,
    partition_by: str = "storeId",
    kpi_name: Optional[str] = None
) -> None:

    kpi_display_name = kpi_name or output_path.split('/')[-1]
    logger.info(f"Generating KPI: {kpi_display_name}")

    try:
        full_query = f"""
            COPY ({query})
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE TRUE)
        """
        con.execute(full_query)
        logger.info(f"Successfully generated KPI: {kpi_display_name}")

    except Exception as e:
        logger.error(f"Failed to generate KPI {kpi_display_name}: {e}")
        raise


# ---------------- MAIN ETL ----------------
def main():
    logger.info("Starting gold layer ETL process")
    con = None

    try:
        con = initialize_duckdb()

        # Example KPI (you can uncomment others as needed)
        generate_kpi(
            con,
            query=f"""
                SELECT *
                FROM read_parquet('{SILVER_PATH}', union_by_name=True)
            """,
            output_path=f'{GOLD_BASE}/storewise_data',
            kpi_name='storewise_data'
        )

        logger.info("✅ ALL GOLD KPIs GENERATED AND PARTITIONED BY storeId")

    except Exception as e:
        logger.error(f"Gold layer ETL process failed: {e}", exc_info=True)
        raise

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed")


# ---------------- SCHEDULER WRAPPER ----------------
def run_gold_job():
    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"🚀 Gold ETL triggered at IST time: {now_ist}")

    try:
        main()
    except Exception:
        logger.error("❌ Gold ETL execution failed", exc_info=True)


# ---------------- SCHEDULER ----------------
if __name__ == "__main__":

    logger.info("🕒 Scheduler started (IST timezone)")

    # Schedule 5 runs daily (IST time)
    schedule.every().day.at("08:00").do(run_gold_job)
    schedule.every().day.at("12:00").do(run_gold_job)
    schedule.every().day.at("16:00").do(run_gold_job)
    schedule.every().day.at("20:00").do(run_gold_job)
    schedule.every().day.at("00:00").do(run_gold_job)

    # Keep script running forever
    while True:
        schedule.run_pending()
        time.sleep(30)
