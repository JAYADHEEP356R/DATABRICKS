import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------------------------
# BRONZE TABLE (RAW INGESTION)
# ---------------------------

@dlt.table(
    name="bronze_orders",
    comment="Raw orders data from CSV"
)
def bronze_orders():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/Volumes/workspace/default/autoloads/raw_data/orders_batch1.csv")
    )

# ---------------------------
# SILVER TABLE (CLEANING)
# ---------------------------

@dlt.table(
    name="silver_orders",
    comment="Cleaned orders data"
)
@dlt.expect("valid_amount", "order_amount > 0")
def silver_orders():
    df = dlt.read("bronze_orders")
    return (
        df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_amount", col("order_amount").cast("double"))
        .filter(col("order_status").isNotNull())
    )

@dlt.view(name="silver_orderid_view")
def silver_orderid_view():
    df = dlt.read("silver_orders")
    return (
        df.select("order_id")
    )

# ---------------------------
# GOLD TABLE (BUSINESS LOGIC)
# ---------------------------

@dlt.table(
    name="gold_orders_summary",
    comment="Aggregated order metrics"
)
def gold_orders_summary():
    df = dlt.read("silver_orders")
    return (
        df.groupBy("order_status")
        .agg(
            count("order_id").alias("total_orders"),
            sum("order_amount").alias("total_revenue")
        )
    )