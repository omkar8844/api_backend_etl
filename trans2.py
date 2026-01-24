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
except Exception as e:
    logger.error(f"Failed to initialize DuckDB Azure extension: {e}")
    raise


def get_read_path(collection_name: str) -> str:
    """
    Generate the read path for a collection from bronze layer.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        Azure blob path string
    """
    return f"az://{CONTAINER_NAME}/bronze/{collection_name}/data/*/*.parquet"


def get_last_load(collection_name: str) -> datetime:
    """
    Get the last load timestamp (watermark) for a collection from silver layer.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        Datetime object representing the watermark
        
    Raises:
        ResourceNotFoundError: If watermark file doesn't exist
        ValueError: If watermark format is invalid
    """
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME,
        blob=f"silver/{collection_name}/watermark/{collection_name}_trans_tm.txt"
    )
    stream = blob_client.download_blob()
    watermark_str = stream.readall().decode("utf-8").strip()
    watermark_dt = datetime.strptime(watermark_str, WATERMARK_DATE_FORMAT)
    logger.info(f"Current watermark for {collection_name}: {watermark_str}")
    return watermark_dt


def load_data() -> pd.DataFrame:
    """
    Load and join data from multiple bronze layer collections.
    
    Returns:
        DataFrame with joined bill data
    """
    logger.info("Starting silver layer data load")
    billRequest = get_read_path('billRequest')
    storeDetails = f'az://{CONTAINER_NAME}/bronze/storeDetails/data/*.parquet'
    invoiceExtractedData = get_read_path('invoiceExtractedData')
    receiptExtractedData = get_read_path("receiptExtractedData")
    billtransactions = get_read_path("billtransactions")
    now = datetime.now().replace(second=0, microsecond=0)
    
    try:
        # Try to get watermarks for incremental load
        br_w = get_last_load("billRequest")
        in_w = get_last_load("invoiceExtractedData")
        rec_w = get_last_load("receiptExtractedData")
        bt_w = get_last_load("billtransactions")
        
        # Check if watermark matches max createdAt from source
        try:
            max_created_at_query = f"""
                SELECT MAX(b.createdAt) AS max_created_at
                FROM read_parquet('{billRequest}') b
            """
            max_created_at_df = con.execute(max_created_at_query).fetch_df()
            if not max_created_at_df.empty and max_created_at_df['max_created_at'].iloc[0] is not None:
                max_created_at = pd.to_datetime(max_created_at_df['max_created_at'].iloc[0])
                # Compare with billRequest watermark (within 1 minute tolerance)
                if abs((max_created_at - br_w).total_seconds()) < 60:
                    logger.info(f"No data to load: Watermark ({br_w}) matches max createdAt ({max_created_at})")
                    return pd.DataFrame()
        except Exception:
            # Silently proceed if check fails
            pass
        query_join_bills = f"""
            SELECT 
                b.billId, 
                i.billId AS in_id, 
                r.billId AS rec_id,
                bt.billId AS bt_id,
                COALESCE(i.billAmount, r.billAmount, bt.billAmount) AS billAmount,
                b.name, 
                b.mobileNumber,
                b.age,
                b.email,
                b.gender,
                b.storeId,
                st.storeName,
                b.createdAt,
                b.trans_time AS load_br,
                i.trans_time AS load_in,
                r.trans_time AS load_rec,
                bt.trans_time AS load_bt
            FROM (
                SELECT * 
                FROM read_parquet('{billRequest}') 
                WHERE trans_time > '{br_w}'
            ) b
            LEFT JOIN (
                SELECT * 
                FROM read_parquet('{invoiceExtractedData}') 
                WHERE trans_time > '{in_w}'
            ) i ON b.billId = i.billId
            LEFT JOIN (
                SELECT * 
                FROM read_parquet('{receiptExtractedData}') 
                WHERE trans_time > '{rec_w}'
            ) r ON b.billId = r.billId
            LEFT JOIN (
                SELECT * 
                FROM read_parquet('{billtransactions}') 
                WHERE trans_time > '{bt_w}'
            ) bt ON b.billId = bt.billId
            LEFT JOIN read_parquet('{storeDetails}') st
                ON b.storeId = st.storeId
        """
            
    except ResourceNotFoundError:
        logger.info("FIRST LOAD: No watermarks found. Performing full load.")
        query_join_bills = f"""
            SELECT 
                b.billId, 
                i.billId AS in_id, 
                r.billId AS rec_id,
                bt.billId AS bt_id,
                COALESCE(i.billAmount, r.billAmount, bt.billAmount) AS billAmount,
                b.name, 
                b.mobileNumber,
                b.age,
                b.email,
                b.gender,
                b.storeId,
                st.storeName,
                b.createdAt,
                b.trans_time AS load_br,
                i.trans_time AS load_in,
                r.trans_time AS load_rec,
                bt.trans_time AS load_bt
            FROM read_parquet('{billRequest}') b
            LEFT JOIN read_parquet('{invoiceExtractedData}') i
                ON b.billId = i.billId
            LEFT JOIN read_parquet('{receiptExtractedData}') r
                ON b.billId = r.billId
            LEFT JOIN read_parquet('{billtransactions}') bt
                ON b.billId = bt.billId
            LEFT JOIN read_parquet('{storeDetails}') st
                ON b.storeId = st.storeId
        """
    except Exception as e:
        logger.error(f"Error loading watermarks: {e}")
        raise
    
    try:
        bill_df = con.execute(query_join_bills).fetch_df()
        
        if not bill_df.empty:
            bill_df['load_time'] = now
            logger.info(f"Number of rows loaded: {len(bill_df)}")
        else:
            logger.info("No data returned from query")
            
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise
    
    return bill_df

def save_watermark(df: pd.DataFrame, name: str, load_time_col: str, 
                   id_filter: Optional[str] = None, container_client=None) -> None:
    """
    Save watermark for a collection.
    
    Args:
        df: DataFrame containing the data
        name: Collection name
        load_time_col: Column name containing the load time
        id_filter: Optional column to filter on (only update if this column is not null)
        container_client: Azure container client
    """
    try:
        if id_filter:
            df_filtered = df[df[id_filter].notna()].copy()
            if df_filtered.empty:
                return
            max_timestamp = df_filtered[load_time_col].max()
        else:
            if load_time_col not in df.columns:
                return
            max_timestamp = df[load_time_col].max()
        
        watermark_blob_name = f"silver/{name}/watermark/{name}_trans_tm.txt"
        max_timestamp_str = str(max_timestamp)
        
        text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
        text_buffer.seek(0)
        watermark_client = container_client.get_blob_client(watermark_blob_name)
        watermark_client.upload_blob(text_buffer.read(), overwrite=True)
        logger.info(f"Updated watermark for {name}: {max_timestamp_str}")
        
    except Exception as e:
        logger.error(f"Failed to save watermark for {name}: {e}")
        raise


def upload_df(df: pd.DataFrame, name: str, partition_col: Optional[str] = None) -> None:
    """
    Upload DataFrame to Azure Blob Storage and update watermarks.
    
    Args:
        df: DataFrame to upload
        name: Name identifier for the dataset
        partition_col: Optional column name to partition by
    """
    if df.empty:
        return
    
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    try:
        if not partition_col:
            # Upload as single file
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            blob_name = f"silver/{name}/data/data_{timestamp}.parquet"
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer.read(), overwrite=True)
        else:
            # Upload partitioned files
            for value, group in df.groupby(partition_col):
                buffer = BytesIO()
                group.to_parquet(buffer, index=False)
                buffer.seek(0)  # Critical: Reset buffer position
                blob_name = f"silver/{name}/data/{partition_col}={value}/data_{timestamp}.parquet"
                
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(buffer.read(), overwrite=False)
        
        # Update watermarks for each source collection
        save_watermark(df, 'billRequest', 'load_br', None, container_client)
        save_watermark(df, 'invoiceExtractedData', 'load_in', 'in_id', container_client)
        save_watermark(df, 'receiptExtractedData', 'load_rec', 'rec_id', container_client)
        save_watermark(df, 'billtransactions', 'load_bt', 'bt_id', container_client)
        
    except Exception as e:
        logger.error(f"Failed to upload data for {name}: {e}")
        raise
    
def main():
    """Main execution function for silver layer ETL."""
    
    try:
        df = load_data()
        if not df.empty:
            # Ensure only unique billIds are saved
            initial_count = len(df)
            df = df.drop_duplicates(subset=['billId'], keep='first')
            final_count = len(df)
            if initial_count != final_count:
                logger.info(f"Deduplication: Removed {initial_count - final_count} duplicate billIds. Rows before: {initial_count}, Rows after: {final_count}")
            upload_df(df=df, name="all_bills", partition_col='storeId')
        else:
            logger.info("No data to process")
    except Exception as e:
        logger.error(f"Silver layer ETL process failed: {e}", exc_info=True)
        raise
    finally:
        con.close()
    
if __name__ == "__main__":
    main()