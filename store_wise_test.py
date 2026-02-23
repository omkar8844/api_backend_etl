import os
import logging
import duckdb
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
    logger.info("🚀 Running gold layer ETL (ONE-TIME RUN)")
    con = None

    try:
        con = initialize_duckdb()

        generate_kpi(
            con,
            query=f"""
                SELECT *
                FROM read_parquet('{SILVER_PATH}', union_by_name=True)
            """,
            output_path=f'{GOLD_BASE}/storewise_data',
            kpi_name='storewise_data'
        )

        logger.info("✅ GOLD ETL COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.error("❌ Gold layer ETL failed", exc_info=True)
        raise

    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed")


# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    main()