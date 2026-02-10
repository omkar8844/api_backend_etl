from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import logging
from typing import Optional
import pandas as pd
load_dotenv()
from datetime import datetime
import duckdb
from azure.core.exceptions import ResourceNotFoundError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable Azure SDK verbose logging
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure').setLevel(logging.WARNING)

# Configuration
AZURE_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONN_STR")
if not AZURE_CONNECTION_STRING:
    raise RuntimeError("AZURE_BLOB_CONN_STR environment variable not set")

CONTAINER_NAME = "clean-data"
CONTAINER_NAME_DEV = "clean-data-dev"
WATERMARK_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Initialize Azure Blob client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# Initialize DuckDB connection
con = duckdb.connect()

try:
    con.execute(f"""CREATE OR REPLACE SECRET secret1 (
        TYPE azure,
        CONNECTION_STRING '{AZURE_CONNECTION_STRING}'
    );""")
    
    con.execute("""
        INSTALL azure;
        LOAD azure;
    """)
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("""
    SET azure_transport_option_type='curl';
    """)
except Exception as e:
    logger.error(f"Failed to initialize DuckDB Azure extension: {e}")
    raise


def get_last_load(collection_name: str, container_name: str) -> datetime:
    """
    Get the last load timestamp (watermark) for a collection from silver layer.
    
    Args:
        collection_name: Name of the collection
        container_name: Name of the container (prod or dev)
        
    Returns:
        Datetime object representing the watermark
        
    Raises:
        ResourceNotFoundError: If watermark file doesn't exist
        ValueError: If watermark format is invalid
    """
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=f"silver/{collection_name}/watermark/{collection_name}_trans_tm.txt"
    )
    stream = blob_client.download_blob()
    watermark_str = stream.readall().decode("utf-8").strip()
    watermark_dt = datetime.strptime(watermark_str, WATERMARK_DATE_FORMAT)
    logger.info(f"Current watermark for {collection_name} ({container_name}): {watermark_str}")
    return watermark_dt


def load_data_prod() -> pd.DataFrame:
    """
    Load items data from production bronze layer with incremental processing.
    
    Returns:
        DataFrame with deduplicated items data
    """
    items_bronze_path = "az://clean-data/items-bronze/data/*/*.parquet"
    now = datetime.now().replace(second=0, microsecond=0)
    
    try:
        # Try to get watermark for incremental load
        watermark_dt = get_last_load("all_items", CONTAINER_NAME)
        
        # Check if watermark matches max createdAt from source
        try:
            max_created_at_query = f"""
                SELECT MAX(createdAt) AS max_created_at
                FROM read_parquet('{items_bronze_path}', union_by_name=True)
            """
            max_created_at_df = con.execute(max_created_at_query).fetch_df()
            if not max_created_at_df.empty and max_created_at_df['max_created_at'].iloc[0] is not None:
                max_created_at = pd.to_datetime(max_created_at_df['max_created_at'].iloc[0])
                # Compare with watermark (within 1 minute tolerance)
                if abs((max_created_at - watermark_dt).total_seconds()) < 60:
                    logger.info(f"No data to load: Watermark ({watermark_dt}) matches max createdAt ({max_created_at})")
                    return pd.DataFrame()
        except Exception:
            # Silently proceed if check fails
            pass
        
        # Incremental load with DISTINCT ON
        query = f"""
            SELECT DISTINCT ON (billId, createdAt, itemName) *
            FROM read_parquet('{items_bronze_path}', union_by_name=True)
            WHERE load_time > '{watermark_dt}'
        """
            
    except ResourceNotFoundError:
        logger.info("FIRST LOAD: No watermark found for production. Performing full load.")
        # Full load with DISTINCT ON
        query = f"""
            SELECT DISTINCT ON (billId, createdAt, itemName) *
            FROM read_parquet('{items_bronze_path}', union_by_name=True)
        """
    except Exception as e:
        logger.error(f"Error loading watermark for production: {e}")
        raise
    
    try:
        items_df = con.execute(query).fetch_df()
        
        if not items_df.empty:
            items_df['load_time'] = now
            logger.info(f"Number of rows loaded for production: {len(items_df)}")
        else:
            logger.info("No new data found for production since watermark")
            
    except Exception as e:
        logger.error(f"Query execution failed for production: {e}")
        raise
    
    return items_df


def load_data_dev() -> pd.DataFrame:
    """
    Load items data from development bronze layer with incremental processing.
    
    Returns:
        DataFrame with deduplicated items data
    """
    items_bronze_path_dev = "az://clean-data-dev/items-bronze/data/*/*.parquet"
    now = datetime.now().replace(second=0, microsecond=0)
    
    try:
        # Try to get watermark for incremental load
        watermark_dt = get_last_load("all_items", CONTAINER_NAME_DEV)
        
        # Check if watermark matches max createdAt from source
        try:
            max_created_at_query = f"""
                SELECT MAX(createdAt) AS max_created_at
                FROM read_parquet('{items_bronze_path_dev}', union_by_name=True)
            """
            max_created_at_df = con.execute(max_created_at_query).fetch_df()
            if not max_created_at_df.empty and max_created_at_df['max_created_at'].iloc[0] is not None:
                max_created_at = pd.to_datetime(max_created_at_df['max_created_at'].iloc[0])
                # Compare with watermark (within 1 minute tolerance)
                if abs((max_created_at - watermark_dt).total_seconds()) < 60:
                    logger.info(f"No data to load: Watermark ({watermark_dt}) matches max createdAt ({max_created_at})")
                    return pd.DataFrame()
        except Exception:
            # Silently proceed if check fails
            pass
        
        # Incremental load with DISTINCT ON
        query = f"""
            SELECT DISTINCT ON (billId, createdAt, itemName) *
            FROM read_parquet('{items_bronze_path_dev}', union_by_name=True)
            WHERE load_time > '{watermark_dt}'
        """
            
    except ResourceNotFoundError:
        logger.info("FIRST LOAD: No watermark found for development. Performing full load.")
        # Full load with DISTINCT ON
        query = f"""
            SELECT DISTINCT ON (billId, createdAt, itemName) *
            FROM read_parquet('{items_bronze_path_dev}', union_by_name=True)
        """
    except Exception as e:
        logger.error(f"Error loading watermark for development: {e}")
        raise
    
    try:
        items_df = con.execute(query).fetch_df()
        
        if not items_df.empty:
            items_df['load_time'] = now
            logger.info(f"Number of rows loaded for development: {len(items_df)}")
        else:
            logger.info("No new data found for development since watermark")
            
    except Exception as e:
        logger.error(f"Query execution failed for development: {e}")
        raise
    
    return items_df


def upload_df(df: pd.DataFrame, name: str, container_name: str) -> None:
    """
    Upload DataFrame to Azure Blob Storage and update watermark.
    
    Args:
        df: DataFrame to upload
        name: Name identifier for the dataset
        container_name: Name of the container (prod or dev)
    """
    if df.empty:
        return
    
    container_client = blob_service_client.get_container_client(container_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    try:
        # Upload as single file (no partitioning)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_name = f"silver/{name}/data/data_{timestamp}.parquet"
        
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer.read(), overwrite=True)
        
        # Update watermark
        if 'load_time' not in df.columns:
            return
            
        max_timestamp = df['load_time'].max()
        watermark_blob_name = f"silver/{name}/watermark/{name}_trans_tm.txt"
        max_timestamp_str = str(max_timestamp)
        
        text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
        text_buffer.seek(0)
        watermark_client = container_client.get_blob_client(watermark_blob_name)
        watermark_client.upload_blob(text_buffer.read(), overwrite=True)
        logger.info(f"Updated watermark for {name} ({container_name}): {max_timestamp_str}")
        
    except Exception as e:
        logger.error(f"Failed to upload data for {name} ({container_name}): {e}")
        raise


def main():
    """Main execution function for silver layer ETL."""
    
    try:
        # Process production data
        df_prod = load_data_prod()
        if not df_prod.empty:
            upload_df(df=df_prod, name="all_items", container_name=CONTAINER_NAME)
        else:
            logger.info("No production data to process")
        
        # Process development data
        df_dev = load_data_dev()
        if not df_dev.empty:
            upload_df(df=df_dev, name="all_items", container_name=CONTAINER_NAME_DEV)
        else:
            logger.info("No development data to process")
            
    except Exception as e:
        logger.error(f"Silver layer ETL process failed: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
