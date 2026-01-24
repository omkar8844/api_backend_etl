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
SOURCE_CONTAINER = "api-analytics"
WATERMARK_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Initialize Azure Blob client (reused across functions)
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
    Generate the read path for a collection from source.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        Azure blob path string
    """
    return f"az://{SOURCE_CONTAINER}/{collection_name}/data/*/*.parquet"

def load_data(collection_name: str, query: str) -> pd.DataFrame:
    """
    Load data from source with incremental processing based on watermark.
    
    Args:
        collection_name: Name of the collection to process
        query: SQL query to execute for data extraction
        
    Returns:
        DataFrame with loaded data, including trans_time column
    """
    now = datetime.now().replace(second=0, microsecond=0)
    
    try:
        # Try to get watermark for incremental load
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=f"bronze/{collection_name}/watermark/{collection_name}_trans_tm.txt"
        )
        stream = blob_client.download_blob()
        watermark_str = stream.readall().decode("utf-8").strip()
        watermark_dt = datetime.strptime(watermark_str, WATERMARK_DATE_FORMAT)
        logger.info(f"Current watermark for {collection_name}: {watermark_str}")
        # Check max createdAt from source to compare with watermark
        try:
            # Extract the FROM clause to get source path
            if "FROM" in query.upper():
                from_part = query.upper().split("FROM")[1].strip()
                # Get the source path (first part before WHERE if exists)
                if "WHERE" in from_part:
                    source_path = from_part.split("WHERE")[0].strip()
                else:
                    source_path = from_part
                # Construct query to get max createdAt
                max_created_at_query = f"SELECT MAX(createdAt) AS max_created_at FROM {source_path}"
                max_created_at_df = con.execute(max_created_at_query).fetch_df()
                if not max_created_at_df.empty and max_created_at_df['max_created_at'].iloc[0] is not None:
                    max_created_at = pd.to_datetime(max_created_at_df['max_created_at'].iloc[0])
                    # Compare watermark with max createdAt (within 1 minute tolerance)
                    if abs((max_created_at - watermark_dt).total_seconds()) < 60:
                        logger.info(f"No data to load: Watermark ({watermark_dt}) matches max createdAt ({max_created_at}) for {collection_name}")
                        return pd.DataFrame()
        except Exception:
            # Silently proceed if check fails
            pass
        
        # Add incremental filter
        inc_ext = f" WHERE load_time > '{watermark_dt}'"
        new_query = query + inc_ext
        
        try:
            df = con.execute(new_query).fetch_df()
        except Exception as e:
            logger.error(f"Query execution failed for {collection_name}: {e}")
            raise
        
        if df.empty:
            logger.info(f"No new data found for {collection_name} since watermark")
        else:
            logger.info(f"Number of rows loaded for {collection_name}: {len(df)}")
            df['trans_time'] = now
            
    except ResourceNotFoundError:
        logger.info(f"FIRST LOAD: No watermark found for {collection_name}. Performing full load.")
        try:
            df = con.execute(query).fetch_df()
            logger.info(f"Number of rows loaded for {collection_name}: {len(df)}")
            df['trans_time'] = now
        except Exception as e:
            logger.error(f"Query execution failed for {collection_name}: {e}")
            raise
    except Exception as e:
        logger.error(f"Unexpected error loading data for {collection_name}: {e}")
        raise
    
    return df


def upload_df(df: pd.DataFrame, name: str, partition_col: Optional[str] = None) -> None:
    """
    Upload DataFrame to Azure Blob Storage and update watermark.
    
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
            blob_name = f"bronze/{name}/data/data_{timestamp}.parquet"
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer.read(), overwrite=True)
        else:
            # Upload partitioned files
            for value, group in df.groupby(partition_col):
                buffer = BytesIO()
                group.to_parquet(buffer, index=False)
                buffer.seek(0)  # Critical: Reset buffer position
                blob_name = f"bronze/{name}/data/{partition_col}={value}/data_{timestamp}.parquet"
                
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(buffer.read(), overwrite=False)
        
        # Update watermark
        if 'load_time' not in df.columns:
            return
            
        max_timestamp = df['load_time'].max()
        watermark_blob_name = f"bronze/{name}/watermark/{name}_trans_tm.txt"
        max_timestamp_str = str(max_timestamp)
        
        text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
        text_buffer.seek(0)
        watermark_client = container_client.get_blob_client(watermark_blob_name)
        watermark_client.upload_blob(text_buffer.read(), overwrite=True)
        logger.info(f"Updated watermark for {name}: {max_timestamp_str}")
        
    except Exception as e:
        logger.error(f"Failed to upload data for {name}: {e}")
        raise
    
def main():
    """Main execution function for bronze layer ETL."""
    
    try:
        # Process storeDetails
        storeDetails = get_read_path("storeDetails")
        query_2 = f"""
            SELECT _id AS storeId, storeName,
                   DATE(createdAt) AS onboard_date, pincode,
                   load_time
            FROM '{storeDetails}'
        """
        store_df = load_data(collection_name='storeDetails', query=query_2)
        if not store_df.empty:
            upload_df(df=store_df, name="storeDetails", partition_col=None)
        
        # Process billRequest
        billRequest = get_read_path("billRequest")
        query_bills = f"""
            SELECT billId, _id, billType, invocationType, posId,
                   storeId, mobileNumber, name,
                   age, email, gender, isWidget, createdAt, load_time
            FROM '{billRequest}'
        """
        bill_df = load_data(collection_name='billRequest', query=query_bills)
        if not bill_df.empty:
            upload_df(bill_df, name='billRequest', partition_col="trans_time")
        
        # Process invoiceExtractedData
        invoiceExtractedData = get_read_path('invoiceExtractedData')
        inx_query = f"""
            SELECT 
                CAST(json_extract(
                    REPLACE(REPLACE(InvoiceTotal, 'None', 'null'), '''', '"'),
                    '$.value'
                ) AS DOUBLE) AS billAmount, 
                billId, tenantId, _id, InvoiceId,
                InvoiceDate, storeId, createdAt, load_time, Items
            FROM '{invoiceExtractedData}'
        """
        inx = load_data(collection_name="invoiceExtractedData", query=inx_query)
        if not inx.empty:
            upload_df(inx, "invoiceExtractedData", "trans_time")
        
        # Process receiptExtractedData
        receiptExtractedData = get_read_path("receiptExtractedData")
        r_query = f"""
            SELECT 
                CAST(
                    json_extract(
                        REPLACE(
                            REPLACE(Total, 'None', 'null'),
                            '''', '"'
                        ),
                        '$.value'
                    ) AS DOUBLE
                ) AS billAmount,
                createdAt,
                load_time,
                storeId,
                tenantId,
                billId,
                Items
            FROM '{receiptExtractedData}'
        """
        rx = load_data('receiptExtractedData', r_query)
        if not rx.empty:
            upload_df(df=rx, name="receiptExtractedData", partition_col="trans_time")
        
        # Process billtransactions
        billtransactions = get_read_path('billtransactions')
        bill_t_query = f"""
            SELECT _id, posId, storeId, billId, custMobile, dob,
                   age, gender, email, name,
                   CAST(billAmount AS DOUBLE) AS billAmount,
                   billItems, paymentMethod,
                   billDate, walletAmount, load_time 
            FROM read_parquet('{billtransactions}', union_by_name=True)
        """
        bill_t = load_data("billtransactions", bill_t_query)
        if not bill_t.empty:
            upload_df(df=bill_t, name="billtransactions", partition_col="trans_time")
        
        
    except Exception as e:
        logger.error(f"Bronze layer ETL process failed: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
