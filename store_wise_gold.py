import os
import logging
import duckdb
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

# Configure logging
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

# Get Azure Blob connection string
conn_str = os.getenv(BLOB_CONN_ENV)
if not conn_str:
    raise RuntimeError(f"{BLOB_CONN_ENV} not set")

def initialize_duckdb() -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB connection with Azure support.
    
    Returns:
        DuckDB connection object
    """
    con = duckdb.connect()
    
    try:
        # Enable Azure support
        con.execute("INSTALL azure;")
        con.execute("LOAD azure;")
        
        # Register Azure secret
        con.execute(
            "CREATE OR REPLACE SECRET blob_secret (TYPE azure, CONNECTION_STRING ?);",
            [conn_str]
        )
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
    #     con.execute("""
    # SET azure_transport_option_type='curl';
    # """)
        
        # Performance tuning (adjust as needed)
        con.execute("PRAGMA threads=4;")
        logger.info("DuckDB initialized successfully with Azure support")
        return con
    except Exception as e:
        logger.error(f"Failed to initialize DuckDB: {e}")
        raise


def generate_kpi(con: duckdb.DuckDBPyConnection, query: str, output_path: str, 
                 partition_by: str = "storeId", kpi_name: Optional[str] = None) -> None:
    """
    Generate and save a KPI to Azure Blob Storage.
    
    Args:
        con: DuckDB connection object
        query: SQL query to execute
        output_path: Output path in Azure Blob Storage
        partition_by: Column to partition by (default: storeId)
        kpi_name: Optional name for logging purposes
    """
    kpi_display_name = kpi_name or output_path.split('/')[-1]
    logger.info(f"Generating KPI: {kpi_display_name}")
    
    try:
        full_query = f"""
            COPY ({query})
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY ({partition_by}),OVERWRITE_OR_IGNORE TRUE)
        """
        con.execute(full_query)
        logger.info(f"Successfully generated KPI: {kpi_display_name}")
    except Exception as e:
        logger.error(f"Failed to generate KPI {kpi_display_name}: {e}")
        raise


def main():
    """Main execution function for gold layer ETL."""
    logger.info("Starting gold layer ETL process")
    con = None
    
    try:
        # Initialize DuckDB connection
        con = initialize_duckdb()
# ---------------------------------------------------------
#38 store_wise_data
        generate_kpi(con,query=f"""
            select * from
            read_parquet('{SILVER_PATH}',union_by_name=True)
                    """,output_path=f'{GOLD_BASE}/storewise_data',
                     kpi_name='storewise_data')


        logger.info("✅ ALL GOLD KPIs GENERATED AND PARTITIONED BY storeId")


 
    except Exception as e:
        logger.error(f"Gold layer ETL process failed: {e}", exc_info=True)
        raise
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed")

if __name__ == "__main__":
    main()