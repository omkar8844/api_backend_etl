from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
load_dotenv()
from datetime import datetime
import duckdb
from azure.core.exceptions import ResourceNotFoundError
import pandas as pd

AZURE_CONNECTION_STRING=os.getenv("AZURE_BLOB_CONN_STR")
blob_service_client=BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

con=duckdb.connect()

con.execute(f"""CREATE SECRET secret1 (
    TYPE azure,
    CONNECTION_STRING '{AZURE_CONNECTION_STRING}'
);""")

con.execute("""
            INSTALL azure;
            LOAD azure;
            """)

def get_read_path(collection_name):
    return f"az://clean-data/bronze/{collection_name}/data/*/*.parquet"


def load_data():
    billRequest=get_read_path('billRequest')
    storeDetails='az://clean-data/bronze/storeDetails/data/*.parquet'
    invoiceExtractedData=get_read_path('invoiceExtractedData')
    receiptExtractedData=get_read_path("receiptExtractedData")
    billtransactions=get_read_path("billtransactions")
    now = datetime.now().replace(second=0, microsecond=0)
    try:
        def get_last_load(collection_name):
            blob_client=blob_service_client.get_blob_client(
                        container="clean-data",
                        blob=f"silver/{collection_name}/watermark/{collection_name}_trans_tm.txt"                                           
                        )
            stream = blob_client.download_blob()
            watermark_str = stream.readall().decode("utf-8").strip()
            print("got the time Hiya")
            print(watermark_str)
            watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%d %H:%M:%S")
            print(f"Watermark found at silver/{collection_name}/watermark/{collection_name}_trans_tm.txt")
            return watermark_dt
        br_w=get_last_load("billRequest")
        in_w=get_last_load("invoiceExtractedData")
        rec_w=get_last_load("receiptExtractedData")
        bt_w=get_last_load("billtransactions")
    #     query_join_bills = f"""
    #     SELECT 
    #         b.billId, 
    #         i.billId AS invoice_id, 
    #         r.billId AS receipt_id,
    #         COALESCE(i.billAmount, r.billAmount,bt.billAmount) AS billAmount,
    #         b.name, 
    #         b.mobileNumber,
    #         b.age,
    #         b.email,
    #         b.gender,
    #         b.storeId,
    #         st.storeName,
    #         b.trans_time as load_br,
    #         i.trans_time as load_in,
    #         r.trans_time as load_rec,
    #         bt.trans_time as load_bt
    #     FROM read_parquet('{billRequest}') b
    #     LEFT JOIN read_parquet('{invoiceExtractedData}') i
    #         ON b.billId = i.billId
    #     LEFT JOIN read_parquet('{receiptExtractedData}') r
    #         ON b.billId = r.billId
    #     LEFT JOIN read_parquet('{billtransactions}') bt
    #         ON b.billId=bt.billId
    #     LEFT JOIN read_parquet('{storeDetails}') st
    #         on b.storeId=st.storeId
    #     where b.trans_time>'{br_w}' and
    #         i.trans_time>'{in_w}' and
    #         r.trans_time>'{rec_w}' and
    #         bt.trans_time>'{bt_w}'

    # """
        query_join_bills = f"""
        SELECT 
            b.billId, 
            i.billId AS in_id, 
            r.billId AS rec_id,
            bt.billId as bt_id,
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
        query_join_bills = f"""
        SELECT 
            b.billId, 
            i.billId AS in_id, 
            r.billId AS rec_id,
            bt.billId as bt_id,
            COALESCE(i.billAmount, r.billAmount,bt.billAmount) AS billAmount,
            b.name, 
            b.mobileNumber,
            b.age,
            b.email,
            b.gender,
            b.storeId,
            st.storeName,
            b.createdAt,
            b.trans_time as load_br,
            i.trans_time as load_in,
            r.trans_time as load_rec,
            bt.trans_time as load_bt
        FROM read_parquet('{billRequest}') b
        LEFT JOIN read_parquet('{invoiceExtractedData}') i
            ON b.billId = i.billId
        LEFT JOIN read_parquet('{receiptExtractedData}') r
            ON b.billId = r.billId
        LEFT JOIN read_parquet('{billtransactions}') bt
            ON b.billId=bt.billId
        LEFT JOIN read_parquet('{storeDetails}') st
            on b.storeId=st.storeId
    """
    #df = con.execute(query_join_bills).fetch_df()
    bill_df=con.execute(query_join_bills).fetch_df()
    if not bill_df.empty:
        bill_df['load_time']=now
        print(f"loaded {len(bill_df)} rows")
    else:
        print("No_Data")
    return bill_df

def upload_df(df,name,partition_col=None):
    print(f"Processing {name}")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client("clean-data")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    buffer = BytesIO()
    if not partition_col:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_name=f"silver/{name}/data/data_{timestamp}.parquet"
        print(blob_name)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(buffer.read(), overwrite=True)
        print(f"Saved partition to blob: {blob_name}")
    else:
        for value, group in df.groupby(partition_col):
        # Serialize group to Parquet in memory
            buffer = BytesIO()
            group.to_parquet(buffer, index=False)
            buffer.seek(0)
            blob_name = f"silver/{name}/data/{partition_col}={value}/data_{timestamp}.parquet"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer, overwrite=False)
    def save_watermark(df,name,load_time,id_filter=None):
        if id_filter:
            df=df[df[id_filter].notna()]
            if df.empty:
                print(f"No valid timestamps for {name}, watermark not updated")
                return
            max_timestamp=df[load_time].max()
            print(f"{max_timestamp} is max load time")
            watermark_blob_name =f"silver/{name}/watermark/{name}_trans_tm.txt"
            max_timestamp_str = str(max_timestamp)
            text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
            watermark_client = container_client.get_blob_client(watermark_blob_name)
            watermark_client.upload_blob(text_buffer.read(), overwrite=True)
        else:
            # df[load_time]=pd.to_datetime(df[load_time],errors='coerce')
            # df=df[df[load_time].notna()]
            max_timestamp=df[load_time].max()
            print(f"{max_timestamp} is max load time")
            watermark_blob_name =f"silver/{name}/watermark/{name}_trans_tm.txt"
            max_timestamp_str = str(max_timestamp)
            text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
            watermark_client = container_client.get_blob_client(watermark_blob_name)
            watermark_client.upload_blob(text_buffer.read(), overwrite=True)
    save_watermark(df,'billRequest','load_br')
    save_watermark(df,'invoiceExtractedData','load_in','in_id')
    save_watermark(df,'receiptExtractedData','load_rec','rec_id')
    save_watermark(df,'billtransactions','load_bt','bt_id')
    
def main():
    df=load_data()
    if not df.empty:
        upload_df(df=df,name="all_bills",partition_col='storeId')
    else:
        print("No data")
    
if __name__ == "__main__":
    main()