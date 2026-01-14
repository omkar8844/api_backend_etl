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
            (FORMAT PARQUET, PARTITION_BY ({partition_by}))
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
        
        # Generate all KPIs

        # ---------------------------------------------------------
        # 1️⃣ DAILY SALES TREND
        # ---------------------------------------------------------
        generate_kpi(con,
            query=f"""
        SELECT
            storeId,
            DATE(createdAt) AS date,
            SUM(billAmount) AS total_sales
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        GROUP BY storeId, DATE(createdAt)
    """,
            output_path=f"{GOLD_BASE}/daily_sales_trend",
            kpi_name="Daily Sales Trend"
        )

# ---------------------------------------------------------
# 2️⃣ MONTHLY SALES TREND
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
            storeId,
            strftime(createdAt, '%Y-%m') AS month,
            SUM(billAmount) AS total_sales
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        GROUP BY storeId, strftime(createdAt, '%Y-%m')
    """,
    output_path=f"{GOLD_BASE}/monthly_sales_trend",
    kpi_name="Monthly Sales Trend"
)

# ---------------------------------------------------------
# 3️⃣ DAILY CUSTOMER VISITS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
            storeId,
            DATE(createdAt) AS date,
            COUNT(DISTINCT mobileNumber) AS customer_visits
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        GROUP BY storeId, DATE(createdAt)
    """,
    output_path=f"{GOLD_BASE}/daily_customer_visits",
    kpi_name="Daily Customer Visits"
)

# ---------------------------------------------------------
# 4️⃣ TOP SPENDERS (PER STORE)
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
            name,
            storeId,
            mobileNumber,
            SUM(billAmount) AS total_spend
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        WHERE LENGTH(mobileNumber) = 10
        GROUP BY name, storeId, mobileNumber
        ORDER BY total_spend DESC
    """,
    output_path=f"{GOLD_BASE}/top_spenders",
    kpi_name="Top Spenders"
)

# ---------------------------------------------------------
# 5️⃣ AVERAGE BILL VALUE (DAILY)
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
            storeId,
            DATE(createdAt) AS date,
            AVG(billAmount) AS avg_bill_value
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        GROUP BY storeId, DATE(createdAt)
    """,
    output_path=f"{GOLD_BASE}/avg_bill_value_daily",
    kpi_name="Average Bill Value Daily"
)

# ---------------------------------------------------------
# 6️⃣ BILL COUNT (DAILY)
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
            storeId,
            DATE(createdAt) AS date,
            COUNT(billId) AS bill_count
        FROM read_parquet('{SILVER_PATH}', union_by_name=True)
        GROUP BY storeId, DATE(createdAt)
    """,
    output_path=f"{GOLD_BASE}/daily_bill_count",
    kpi_name="Daily Bill Count"
)


# ---------------------------------------------------------
# 7️⃣ CUSTOMER LIFETIME VALUE (CLTV)
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
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
    """,
    output_path=f"{GOLD_BASE}/cltv",
    kpi_name="Customer Lifetime Value"
)

# ---------------------------------------------------------
# 8️⃣ AVG_VISIT_WEEK_DAY
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT
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
            GROUP BY storeId, DATE(createdAt), DAYNAME(createdAt)
        )
        GROUP BY storeId, Weekday
        ORDER BY avg_visits DESC
    """,
    output_path=f"{GOLD_BASE}/avg_week",
    kpi_name="Average Visit by Weekday"
)


# ---------------------------------------------------------
# 9️⃣ CHURN ANALYSIS (60 DAY)
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        WITH customer_last_visit AS (
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
    """,
    output_path=f"{GOLD_BASE}/churn_60day",
    kpi_name="Churn Analysis 60 Day"
)
# ---------------------------------------------------------
# 🔟 MONTHLY VISITS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT 
            storeId, 
            strftime(createdAt, '%Y-%m'),
            COUNT(DISTINCT mobileNumber) AS no_of_customers
        FROM read_parquet('{SILVER_PATH}', union_by_name=true)
        GROUP BY storeId, strftime(createdAt, '%Y-%m')
    """,
    output_path=f"{GOLD_BASE}/monthly_visits",
    kpi_name="Monthly Visits"
)

# ---------------------------------------------------------
# 1️⃣1️⃣ MONTHLY BILLS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT 
            storeId, 
            MONTHNAME(createdAt) AS month_name,
            COUNT(DISTINCT billId) AS no_of_bills
        FROM read_parquet('{SILVER_PATH}', union_by_name=true)
        GROUP BY storeId, MONTHNAME(createdAt)
    """,
    output_path=f"{GOLD_BASE}/monthly_bills",
    kpi_name="Monthly Bills"
)

# ---------------------------------------------------------
# 1️⃣2️⃣ AVERAGE HOURLY BILLING TREND
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
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
    """,
    output_path=f"{GOLD_BASE}/Avg_Hourly_Billing_Trend",
    kpi_name="Average Hourly Billing Trend"
)

# ---------------------------------------------------------
# 1️⃣3️⃣ BILL COUNT WINDOWS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
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
            COUNT(CASE WHEN bill_date = CURRENT_DATE THEN 1 END) AS bills_today,
            -- Last 2 days (today + yesterday)
            COUNT(CASE WHEN bill_date >= CURRENT_DATE - 1 THEN 1 END) AS bills_last_2_days,
            -- Previous 2 days
            COUNT(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 3 AND CURRENT_DATE - 2
                THEN 1
            END) AS bills_prev_2_days,
            -- Last 7 days
            COUNT(CASE WHEN bill_date >= CURRENT_DATE - 6 THEN 1 END) AS bills_last_7_days,
            -- Previous 7 days
            COUNT(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 13 AND CURRENT_DATE - 7
                THEN 1
            END) AS bills_prev_7_days,
            -- Last 1 month (~30 days)
            COUNT(CASE WHEN bill_date >= CURRENT_DATE - 30 THEN 1 END) AS bills_last_1_month,
            -- Previous 1 month
            COUNT(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
                THEN 1
            END) AS bills_prev_1_month,
            -- Last 90 days
            COUNT(CASE WHEN bill_date >= CURRENT_DATE - 90 THEN 1 END) AS bills_last_90_days,
            -- Previous 90 days
            COUNT(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 91
                THEN 1
            END) AS bills_prev_90_days,
            -- Lifetime
            COUNT(*) AS bills_lifetime
        FROM base
        GROUP BY storeId
    """,
    output_path=f"{GOLD_BASE}/bill_count_windows",
    kpi_name="Bill Count Windows"
)

# ---------------------------------------------------------
# 1️⃣4️⃣ SALES WINDOWS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        WITH base AS (
            SELECT
                storeId,
                DATE(createdAt) AS bill_date,
                billAmount
            FROM read_parquet('{SILVER_PATH}', union_by_name=true)
            WHERE storeId IS NOT NULL
        )
        SELECT
            storeId,
            -- Today
            COALESCE(SUM(CASE 
                WHEN bill_date = CURRENT_DATE 
                THEN billAmount 
            END), 0) AS sales_today,
            -- Last 2 days (today + yesterday)
            COALESCE(SUM(CASE 
                WHEN bill_date >= CURRENT_DATE - 1 
                THEN billAmount 
            END), 0) AS sales_last_2_days,
            -- Previous 2 days
            COALESCE(SUM(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 3 AND CURRENT_DATE - 2
                THEN billAmount
            END), 0) AS sales_prev_2_days,
            -- Last 7 days
            COALESCE(SUM(CASE 
                WHEN bill_date >= CURRENT_DATE - 6 
                THEN billAmount 
            END), 0) AS sales_last_7_days,
            -- Previous 7 days
            COALESCE(SUM(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 13 AND CURRENT_DATE - 7
                THEN billAmount
            END), 0) AS sales_prev_7_days,
            -- Last 1 month (~30 days)
            COALESCE(SUM(CASE 
                WHEN bill_date >= CURRENT_DATE - 30 
                THEN billAmount 
            END), 0) AS sales_last_1_month,
            -- Previous 1 month
            COALESCE(SUM(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31
                THEN billAmount
            END), 0) AS sales_prev_1_month,
            -- Last 90 days
            COALESCE(SUM(CASE 
                WHEN bill_date >= CURRENT_DATE - 90 
                THEN billAmount 
            END), 0) AS sales_last_90_days,
            -- Previous 90 days
            COALESCE(SUM(CASE
                WHEN bill_date BETWEEN CURRENT_DATE - 180 AND CURRENT_DATE - 91
                THEN billAmount
            END), 0) AS sales_prev_90_days,
            -- Lifetime sales
            COALESCE(SUM(billAmount), 0) AS sales_lifetime
        FROM base
        GROUP BY storeId
    """,
    output_path=f"{GOLD_BASE}/sales_windows",
    kpi_name="Sales Windows"
)




# ---------------------------------------------------------
# 1️⃣5️⃣ ACTIVE CUSTOMER PERCENTAGE
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        WITH base AS (
            SELECT
                storeId,
                mobileNumber,
                STRFTIME(DATE(createdAt), '%Y-%m') AS year_month
            FROM read_parquet('{SILVER_PATH}', union_by_name=true)
            WHERE LENGTH(mobileNumber) = 10
        ),
        latest_month AS (
            SELECT
                storeId,
                MAX(year_month) AS max_year_month
            FROM base
            GROUP BY storeId
        ),
        customer_stats AS (
            SELECT
                b.storeId,
                b.mobileNumber,
                -- lifetime visits
                COUNT(*) AS lifetime_visits,
                -- active in latest month flag
                MAX(CASE
                    WHEN b.year_month = lm.max_year_month THEN 1
                    ELSE 0
                END) AS is_active
            FROM base b
            JOIN latest_month lm
                ON b.storeId = lm.storeId
            GROUP BY b.storeId, b.mobileNumber
        )
        SELECT
            storeId,
            COUNT(*) AS total_customers,
            -- Active customers
            SUM(is_active) AS active_customers,
            ROUND(
                100.0 * SUM(is_active) / COUNT(*),
                2
            ) AS active_customer_percentage,
            -- Repeat customers (lifetime_visits >= 2)
            COUNT(CASE
                WHEN lifetime_visits >= 2 THEN 1
            END) AS repeat_customers,
            ROUND(
                100.0 * COUNT(CASE WHEN lifetime_visits >= 2 THEN 1 END) / COUNT(*),
                2
            ) AS repeat_customer_percentage
        FROM customer_stats
        GROUP BY storeId
    """,
    output_path=f"{GOLD_BASE}/active_cust_pct",
    kpi_name="Active Customer Percentage"
)


# ---------------------------------------------------------
# 1️⃣6️⃣ RFM SEGMENTS
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        WITH base AS (
            SELECT
                storeId,
                mobileNumber,
                name,
                DATE(createdAt) AS bill_date,
                billAmount
            FROM read_parquet('{SILVER_PATH}', union_by_name=true)
            WHERE storeId IS NOT NULL
              AND LENGTH(mobileNumber) = 10
        ),
        customer_agg AS (
            SELECT
                storeId,
                name,
                mobileNumber,
                DATE_DIFF('day', MAX(bill_date), CURRENT_DATE) AS recency_days,
                COUNT(*) AS frequency,
                SUM(billAmount) AS monetary
            FROM base
            GROUP BY storeId, mobileNumber,name
        ),
        ranked AS (
            SELECT
                *,
                PERCENT_RANK() OVER (PARTITION BY storeId ORDER BY recency_days ASC) AS r_pct,
                PERCENT_RANK() OVER (PARTITION BY storeId ORDER BY frequency DESC) AS f_pct,
                PERCENT_RANK() OVER (PARTITION BY storeId ORDER BY monetary DESC) AS m_pct
            FROM customer_agg
        ),
        scored AS (
            SELECT
                *,
                CASE
                    WHEN r_pct <= 0.10 THEN 5
                    WHEN r_pct <= 0.30 THEN 4
                    WHEN r_pct <= 0.60 THEN 3
                    WHEN r_pct <= 0.80 THEN 2
                    ELSE 1
                END AS r_score,
                CASE
                    WHEN f_pct <= 0.10 THEN 5
                    WHEN f_pct <= 0.30 THEN 4
                    WHEN f_pct <= 0.60 THEN 3
                    WHEN f_pct <= 0.80 THEN 2
                    ELSE 1
                END AS f_score,
                CASE
                    WHEN m_pct <= 0.10 THEN 5
                    WHEN m_pct <= 0.30 THEN 4
                    WHEN m_pct <= 0.60 THEN 3
                    WHEN m_pct <= 0.80 THEN 2
                    ELSE 1
                END AS m_score
            FROM ranked
        )
        SELECT
            storeId,
            mobileNumber,
            name,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            CONCAT(r_score, f_score, m_score) AS rfm_segment,
            (r_score + f_score + m_score) AS rfm_score,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4
                    THEN 'Star Customers'
                WHEN f_score >= 4 AND r_score >= 3
                    THEN 'Loyal Customers'
                WHEN m_score >= 4 AND r_score >= 3
                    THEN 'Big Spenders'
                WHEN r_score = 5 AND f_score = 1
                    THEN 'New Customers'
                WHEN r_score >= 4 AND f_score = 2
                    THEN 'Potential Loyalists'
                WHEN r_score <= 2 AND f_score >= 3
                    THEN 'At Risk'
                WHEN r_score <= 2 AND f_score <= 2
                    THEN 'Hibernating'
                ELSE 'Others'
            END AS customer_segment
        FROM scored
    """,
    output_path=f"{GOLD_BASE}/rfm_segments",
    kpi_name="RFM Segments"
)


# ---------------------------------------------------------
# 1️⃣7️⃣ CUSTOMER SEGMENT COUNT
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT 
            storeId,
            customer_segment,
            COUNT(DISTINCT mobileNumber) AS count_of_customers
        FROM read_parquet('{GOLD_BASE}/rfm_segments/*/data_0.parquet')
        GROUP BY storeId, customer_segment
    """,
    output_path=f"{GOLD_BASE}/cust_segment_count",
    kpi_name="Customer Segment Count"
)

# ---------------------------------------------------------
# 1️⃣8️⃣ CUSTOMER SEGMENT SPEND
# ---------------------------------------------------------
        generate_kpi(con,
    query=f"""
        SELECT 
            storeId,
            customer_segment,
            SUM(monetary) AS total_amount_spent
        FROM read_parquet('{GOLD_BASE}/rfm_segments/*/data_0.parquet')
        GROUP BY storeId, customer_segment
        ORDER BY storeId, total_amount_spent DESC
    """,
    output_path=f"{GOLD_BASE}/cust_segment_spend",
    kpi_name="Customer Segment Spend"
)

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

