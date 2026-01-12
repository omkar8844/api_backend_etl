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
    logger.info("DuckDB Azure extension loaded successfully")
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
    logger.info(f"Processing collection: {collection_name}")
    now = datetime.now().replace(second=0, microsecond=0)
    
    try:
        # Try to get watermark for incremental load
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=f"bronze/{collection_name}/watermark/{collection_name}_trans_tm.txt"
        )
        stream = blob_client.download_blob()
        watermark_str = stream.readall().decode("utf-8").strip()
        logger.info(f"Watermark found for {collection_name}: {watermark_str}")
        
        try:
            watermark_dt = datetime.strptime(watermark_str, WATERMARK_DATE_FORMAT)
            logger.info(f"Parsed watermark datetime: {watermark_dt}")
        except ValueError as e:
            logger.error(f"Invalid watermark format for {collection_name}: {watermark_str}. Error: {e}")
            raise ValueError(f"Invalid watermark format. Expected {WATERMARK_DATE_FORMAT}, got: {watermark_str}")
        
        # Add incremental filter
        inc_ext = f" WHERE load_time > '{watermark_dt}'"
        new_query = query + inc_ext
        logger.debug(f"Executing incremental query for {collection_name}")
        
        try:
            df = con.execute(new_query).fetch_df()
        except Exception as e:
            logger.error(f"Query execution failed for {collection_name}: {e}")
            raise
        
        if df.empty:
            logger.info(f"No new data found for {collection_name} since watermark")
        else:
            logger.info(f"Loaded {len(df)} new rows for {collection_name}")
            df['trans_time'] = now
            
    except ResourceNotFoundError:
        logger.info(f"No watermark found for {collection_name}. Performing full load.")
        try:
            df = con.execute(query).fetch_df()
            logger.info(f"Full load completed: {len(df)} rows loaded for {collection_name}")
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
        logger.warning(f"Empty DataFrame provided for {name}. Skipping upload.")
        return
    
    logger.info(f"Uploading data for {name}")
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    try:
        if not partition_col:
            # Upload as single file
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            blob_name = f"bronze/{name}/data/data_{timestamp}.parquet"
            logger.debug(f"Uploading to: {blob_name}")
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer.read(), overwrite=True)
            logger.info(f"Successfully uploaded to blob: {blob_name}")
        else:
            # Upload partitioned files
            for value, group in df.groupby(partition_col):
                buffer = BytesIO()
                group.to_parquet(buffer, index=False)
                buffer.seek(0)  # Critical: Reset buffer position
                blob_name = f"bronze/{name}/data/{partition_col}={value}/data_{timestamp}.parquet"
                logger.debug(f"Uploading partition {partition_col}={value} to: {blob_name}")
                
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(buffer.read(), overwrite=False)
                logger.info(f"Successfully uploaded partition to blob: {blob_name}")
        
        # Update watermark
        if 'load_time' not in df.columns:
            logger.warning(f"'load_time' column not found in {name}. Cannot update watermark.")
            return
            
        max_timestamp = df['load_time'].max()
        watermark_blob_name = f"bronze/{name}/watermark/{name}_trans_tm.txt"
        max_timestamp_str = str(max_timestamp)
        
        text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
        text_buffer.seek(0)
        watermark_client = container_client.get_blob_client(watermark_blob_name)
        watermark_client.upload_blob(text_buffer.read(), overwrite=True)
        logger.info(f"Watermark updated for {name}: {max_timestamp_str}")
        
    except Exception as e:
        logger.error(f"Failed to upload data for {name}: {e}")
        raise
    
def main():
    """Main execution function for bronze layer ETL."""
    logger.info("Starting bronze layer ETL process")
    
    try:
        # Process storeDetails
        logger.info("Processing storeDetails")
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
        else:
            logger.info("No new data to upload for storeDetails")
        
        # Process billRequest
        logger.info("Processing billRequest")
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
        else:
            logger.info("No new data to upload for billRequest")
        
        # Process invoiceExtractedData
        logger.info("Processing invoiceExtractedData")
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
        else:
            logger.info("No new data to upload for invoiceExtractedData")
        
        # Process receiptExtractedData
        logger.info("Processing receiptExtractedData")
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
        else:
            logger.info("No new data to upload for receiptExtractedData")
        
        # Process billtransactions
        logger.info("Processing billtransactions")
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
        else:
            logger.info("No new data to upload for billtransactions")
        
        logger.info("Bronze layer ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"Bronze layer ETL process failed: {e}", exc_info=True)
        raise
    finally:
        con.close()
        logger.info("DuckDB connection closed")


if __name__ == "__main__":
    main()
