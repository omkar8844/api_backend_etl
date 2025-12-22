from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
load_dotenv()
from datetime import datetime
import duckdb
from azure.core.exceptions import ResourceNotFoundError

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

def get__read_path(collection_name):
    return f"az://api-analytics/{collection_name}/data/*/*.parquet"

def load_data(collection_name,query):
    print(f"Processing {collection_name}")
    now = datetime.now().replace(second=0, microsecond=0)
    try:
        blob_client=blob_service_client.get_blob_client(
                    container="clean-data",
                    blob=f"bronze/{collection_name}/watermark/{collection_name}_trans_tm.txt"                                           
                    )
        stream = blob_client.download_blob()
        watermark_str = stream.readall().decode("utf-8").strip()
        print("got the time Hiya")
        print(watermark_str)
        watermark_dt = datetime.strptime(watermark_str, "%Y-%m-%d %H:%M:%S")
        print(watermark_dt)
        inc_ext=f"where load_time>'{watermark_dt}'"
        new_query=query+inc_ext
        df=con.execute(new_query).fetch_df()
        if df.empty:
            print("Empty df")
        else:
            print(f"New {len(df)} rows loaded")
            df['trans_time']=now
    except ResourceNotFoundError:
        print("Hiya, New data")
        df=con.execute(query).fetch_df()
        print(f"loaded {len(df)} rows")
        df['trans_time']=now
    return df


def upload_df(df,name,partition_col=None):
    print(f"Processing {name}")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client("clean-data")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    buffer = BytesIO()
    if not partition_col:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_name=f"bronze/{name}/data/data_{timestamp}.parquet"
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
            blob_name = f"bronze/{name}/data/{partition_col}={value}/data_{timestamp}.parquet"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(buffer, overwrite=False)
    max_timestamp=df['load_time'].max()
    watermark_blob_name =f"bronze/{name}/watermark/{name}_trans_tm.txt"
    max_timestamp_str = str(max_timestamp)
    text_buffer = BytesIO(max_timestamp_str.encode("utf-8"))
    watermark_client = container_client.get_blob_client(watermark_blob_name)
    watermark_client.upload_blob(text_buffer.read(), overwrite=True)
    
    
storeDetails=get__read_path("storeDetails")
query_2=f"""
            select _id as storeId, storeName,
            DATE(createdAt) as onboard_date,pincode,
            load_time
            from '{storeDetails}'
            """
store_df=load_data(collection_name='storeDetails',query=query_2)
if not store_df.empty:
    upload_df(df=store_df,name="storeDetails",partition_col=None)
else:
    print("Nothing to upload")
#---------------------------------------------------------------------------
billReuest=get__read_path("billRequest")
query_bills=f"""
    select billId,_id,billType,invocationType,posId,
    storeId, mobileNumber, name,
    age,email,gender,isWidget,createdAt,load_time
    from '{billReuest}'
"""
bill_df=load_data(collection_name='billRequest',query=query_bills)    
if not bill_df.empty:
    upload_df(bill_df,name='billRequest',partition_col="trans_time")
else:
    print("Nothing to upload")
#---------------------------------------------------------------------------    
invoiceExtractedData=get__read_path('invoiceExtractedData')
inx_query=f"""
SELECT 
  CAST(json_extract(
    replace(replace(InvoiceTotal, 'None', 'null'), '''', '"'),
    '$.value'
  ) AS DOUBLE) AS billAmount, billId, tenantId, _id,InvoiceId,
  InvoiceDate,storeId,createdAt, load_time, Items
FROM
 '{invoiceExtractedData}'
"""
inx=load_data(collection_name="invoiceExtractedData",query=inx_query)
if not inx.empty:
    upload_df(inx,"invoiceExtractedData","trans_time")
else:
    print("Nothing to upload")
#------------------------------------------------------------------------------------------
receiptExtractedData=get__read_path("receiptExtractedData")
# r_query=f"""
#             select json_extract
#             CAST((
#                 replace(
#                     replace(
#                         Total,'None','null'
#                     ),'''','"'
#                 ),
#                 '$.value'
#             )AS DOUBLE) as billAmount,createdAt,load_time,storeId,tenantId,billId
#             from '{receiptExtractedData}'
#             """
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
        billId
    FROM '{receiptExtractedData}'
"""
rx=load_data('receiptExtractedData',r_query)
if not rx.empty:
    upload_df(df=rx,name="receiptExtractedData",partition_col="trans_time")
else:
    print("Nothing to upload")
#----------------------------------------------------------------------------------
billtransactions=get__read_path('billtransactions')
bill_t_query=f"""
    select _id, posId, storeId, billId, custMobile,dob,
    age,gender,email,name,
    
    cast(billAmount as double) as billAmount
    
    ,billItems,paymentMethod,
    billDate,walletAmount,load_time 
    from read_parquet('{billtransactions}', union_by_name=True)

"""

bill_t=load_data("billtransactions",bill_t_query)

if not bill_t.empty:
    upload_df(df=bill_t,name="billtransactions",partition_col="trans_time")
else:
    print("Nothing to upload")
