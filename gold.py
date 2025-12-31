import os
import duckdb
from dotenv import load_dotenv
load_dotenv()


# ---------------- CONFIG ----------------
SILVER_PATH = "az://clean-data/silver/all_bills/data/*/*.parquet"
GOLD_BASE   = "az://clean-data/gold"
BLOB_CONN_ENV = "AZURE_BLOB_CONN_STR"
# ----------------------------------------

# Get Azure Blob connection string
conn_str = os.getenv(BLOB_CONN_ENV)
if not conn_str:
    raise RuntimeError(f"{BLOB_CONN_ENV} not set")

# Connect to DuckDB
con = duckdb.connect()

# Enable Azure support
con.execute("INSTALL azure;")
con.execute("LOAD azure;")

# Register Azure secret
con.execute(
    "CREATE OR REPLACE SECRET blob_secret (TYPE azure, CONNECTION_STRING ?);",
    [conn_str]
)

# Performance tuning (adjust as needed)
con.execute("PRAGMA threads=4;")

# # ---------------------------------------------------------
# # 1️⃣ DAILY SALES TREND
# # ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        DATE(createdAt) AS date,
        SUM(billAmount) AS total_sales
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    GROUP BY storeId, DATE(createdAt)
)
TO '{GOLD_BASE}/daily_sales_trend'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 2️⃣ MONTHLY SALES TREND
# ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        DATE_TRUNC('month', createdAt) AS month,
        SUM(billAmount) AS total_sales
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    GROUP BY storeId, DATE_TRUNC('month', createdAt)
)
TO '{GOLD_BASE}/monthly_sales_trend'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 3️⃣ DAILY CUSTOMER VISITS
# ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        DATE(createdAt) AS date,
        COUNT(DISTINCT mobileNumber) AS customer_visits
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    GROUP BY storeId, DATE(createdAt)
)
TO '{GOLD_BASE}/daily_customer_visits'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 4️⃣ TOP SPENDERS (PER STORE)
# ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        mobileNumber,
        SUM(billAmount) AS total_spend
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    WHERE LENGTH(mobileNumber) = 10
    GROUP BY storeId, mobileNumber
)
TO '{GOLD_BASE}/top_spenders'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 5️⃣ AVERAGE BILL VALUE (DAILY)
# ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        DATE(createdAt) AS date,
        AVG(billAmount) AS avg_bill_value
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    GROUP BY storeId, DATE(createdAt)
)
TO '{GOLD_BASE}/avg_bill_value_daily'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 6️⃣ BILL COUNT (DAILY)
# ---------------------------------------------------------
con.execute(f"""
COPY (
    SELECT
        storeId,
        DATE(createdAt) AS date,
        COUNT(billId) AS bill_count
    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    GROUP BY storeId, DATE(createdAt)
)
TO '{GOLD_BASE}/daily_bill_count'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")


# ---------------------------------------------------------
# 7️⃣ CUSTOMER LIFETIME VALUE (CLTV)
# ---------------------------------------------------------

con.execute(f"""
COPY (
    SELECT
        storeId,
        mobileNumber,

        -- total amount spent
        SUM(billAmount) AS total_spend,

        -- number of invoices
        COUNT(billId) AS transaction_count,

        -- average bill
        AVG(billAmount) AS avg_bill_value,

        -- first and last purchase
        MIN(createdAt) AS first_purchase,
        MAX(createdAt) AS last_purchase,

        -- lifetime in days
        GREATEST(
            DATE_DIFF('day', MIN(createdAt), MAX(createdAt)),
            1  -- avoid divide by zero
        ) AS lifetime_days,

        -- frequency = transactions / days active
        (COUNT(billId) :: DOUBLE) 
            / GREATEST(
                DATE_DIFF('day', MIN(createdAt), MAX(createdAt)),
                1
            ) AS purchase_frequency,

        -- SAFE CLTV formulation with upper bound & float cast
        CAST(
            LEAST(
                SUM(billAmount) *
                (
                    (COUNT(billId) :: DOUBLE)
                    / GREATEST(
                        DATE_DIFF('day', MIN(createdAt), MAX(createdAt)),
                        1
                    )
                ),
                1e12   -- prevent float overflow beyond json range
            )
            AS DOUBLE
        ) AS cltv_value

    FROM read_parquet('{SILVER_PATH}', union_by_name=True)
    WHERE LENGTH(mobileNumber) = 10     -- avoid null/bad customer ids
    GROUP BY storeId, mobileNumber
)
TO '{GOLD_BASE}/cltv'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")

# ---------------------------------------------------------
# 8 AVG_VISIT_WEEK_DAY
# ---------------------------------------------------------
con.execute(f"""
COPY(    SELECT
        weekday AS Weekday,
        AVG(daily_visitors) AS avg_visits,
        storeId
    FROM (
        SELECT
            DATE(createdAt) AS day,
            DAYNAME(createdAt) AS weekday,
            COUNT(DISTINCT mobileNumber) AS daily_visitors,
            storeId
        FROM read_parquet('{SILVER_PATH}', union_by_name=true)
        GROUP BY storeId,DATE(createdAt), DAYNAME(createdAt)
    )
    GROUP BY storeId, Weekday
    ORDER BY avg_visits DESC)
    TO '{GOLD_BASE}/avg_week'
    (
    FORMAT PARQUET,
    PARTITION_BY (storeId)
    );
""")


# ---------------------------------------------------------
# 9 AVG_VISIT_WEEK_DAY
# ---------------------------------------------------------

con.execute(f"""
copy(WITH customer_last_visit AS (
    SELECT
        storeId,
        mobileNumber,
        MAX(createdAt) AS last_purchase
    FROM read_parquet('{SILVER_PATH}', union_by_name=true)
    WHERE LENGTH(mobileNumber)=10
    GROUP BY storeId, mobileNumber
),
churn_flagged AS (
    SELECT
        storeId,
        mobileNumber,
        last_purchase,
        CASE
            WHEN last_purchase < (CURRENT_DATE - INTERVAL '60' DAY)
            THEN 1
            ELSE 0
        END AS is_churned
    FROM customer_last_visit
)
SELECT
    storeId,
    COUNT(*) AS total_customers,
    SUM(is_churned) AS churned_customers,
    100.0 * SUM(is_churned) / COUNT(*) AS churn_rate_percent
FROM churn_flagged
GROUP BY storeId
) TO '{GOLD_BASE}/churn_60day'
(
    format parquet,
    partition_by(storeId)
)
""")
# ---------------------------------------------------------
# 10 Monthly bills
# ---------------------------------------------------------

con.execute(f"""
    copy(           
        select storeId, MONTHNAME(createdAt),(count(distinct(mobileNumber))) as 'No of Customers' 
        from read_parquet('{SILVER_PATH}',union_by_name=true)
        group by 1,2
    )to '{GOLD_BASE}/monthly_visits'
    (
        format parquet,
        partition_by(storeId)
    )            
            """)

# ---------------------------------------------------------
# 11 Monthly bills
# ---------------------------------------------------------
con.execute(f"""         
        copy(select storeId, MONTHNAME(createdAt),(count(distinct(billId))) as 'No. of Bills' 
        from read_parquet('{SILVER_PATH}',union_by_name=true)
        group by 1,2)
        to '{GOLD_BASE}/monthly_bills'
    (
        format parquet,
        partition_by(storeId)
    )            
      """)

# ---------------------------------------------------------
# 12 avg_hourly
# ---------------------------------------------------------


con.execute(f"""
    COPY
    (
        WITH hourly_daily AS (
    SELECT
        storeId,
        DATE(createdAt) AS bill_date,
        EXTRACT(HOUR FROM createdAt) AS hour_of_day,
        COUNT(*) AS bills_in_hour
    FROM read_parquet('{SILVER_PATH}', union_by_name=true)
    GROUP BY storeId, DATE(createdAt), EXTRACT(HOUR FROM createdAt)
)
SELECT
    storeId,
    hour_of_day,
    AVG(bills_in_hour) AS avg_bill_count
FROM hourly_daily
GROUP BY storeId, hour_of_day
ORDER BY storeId, hour_of_day
)
 TO '{GOLD_BASE}/Avg_Hourly_Billing_Trend'
 (
     format parquet,
     partition_by(storeId)
 )
""")

# ---------------------------------------------------------
# 13 bill_count_daily
# ---------------------------------------------------------

# con.execute(f"""
# COPY (
#     WITH base AS (
#         SELECT
#             storeId,
#             DATE(createdAt) AS bill_date   -- 🔑 normalize ONCE
#         FROM read_parquet('{SILVER_PATH}', union_by_name=true)
#         WHERE storeId IS NOT NULL
#     )
#     SELECT
#         storeId,

#         -- Today
#         COUNT(CASE
#             WHEN bill_date = CURRENT_DATE THEN 1
#         END) AS bills_today,

#         -- Last 2 days (today + yesterday)
#         COUNT(CASE
#             WHEN bill_date >= CURRENT_DATE - 1 THEN 1
#         END) AS bills_last_2_days,

#         -- Last 7 days
#         COUNT(CASE
#             WHEN bill_date >= CURRENT_DATE - 6 THEN 1
#         END) AS bills_last_7_days,

#         -- Last 1 month (~30 days)
#         COUNT(CASE
#             WHEN bill_date >= CURRENT_DATE - 30 THEN 1
#         END) AS bills_last_1_month,

#         -- Last 90 days
#         COUNT(CASE
#             WHEN bill_date >= CURRENT_DATE - 90 THEN 1
#         END) AS bills_last_90_days,

#         -- Lifetime (ALL data)
#         COUNT(*) AS bills_lifetime

#     FROM base
#     GROUP BY storeId
# )
# TO '{GOLD_BASE}/bill_count_windows'
# (
#     FORMAT PARQUET,
#     PARTITION_BY (storeId)
# );
# """)
con.execute(f"""
COPY (
    WITH base AS (
        SELECT
            storeId,
            DATE(createdAt) AS bill_date
        FROM read_parquet('{SILVER_PATH}', union_by_name=true)
        WHERE storeId IS NOT NULL
    )
    SELECT
        storeId,

        -- Today
        COUNT(CASE WHEN bill_date = CURRENT_DATE THEN 1 END)
            AS bills_today,

        -- Last 2 days (today + yesterday)
        COUNT(CASE WHEN bill_date >= CURRENT_DATE - 1 THEN 1 END)
            AS bills_last_2_days,

        -- Previous 2 days
        COUNT(CASE
            WHEN bill_date BETWEEN CURRENT_DATE - 3 AND CURRENT_DATE - 2
            THEN 1
        END) AS bills_prev_2_days,

        -- Last 7 days
        COUNT(CASE WHEN bill_date >= CURRENT_DATE - 6 THEN 1 END)
            AS bills_last_7_days,

        -- Previous 7 days
        COUNT(CASE
            WHEN bill_date BETWEEN CURRENT_DATE - 13 AND CURRENT_DATE - 7
            THEN 1
        END) AS bills_prev_7_days,

        -- Last 1 month (~30 days)
        COUNT(CASE WHEN bill_date >= CURRENT_DATE - 30 THEN 1 END)
            AS bills_last_1_month,

        -- Previous 1 month
        COUNT(CASE
            WHEN bill_date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
            THEN 1
        END) AS bills_prev_1_month,

        -- Last 90 days
        COUNT(CASE WHEN bill_date >= CURRENT_DATE - 90 THEN 1 END)
            AS bills_last_90_days,

        -- Previous 90 days
        COUNT(CASE
            WHEN bill_date BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 91
            THEN 1
        END) AS bills_prev_90_days,

        -- Lifetime
        COUNT(*) AS bills_lifetime

    FROM base
    GROUP BY storeId
)
TO '{GOLD_BASE}/bill_count_windows'
(
    FORMAT PARQUET,
    PARTITION_BY (storeId)
);
""")




con.close()

print("✅ ALL GOLD KPIs GENERATED AND PARTITIONED BY storeId")

