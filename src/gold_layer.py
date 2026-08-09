# Databricks notebook source
from pyspark.sql.functions import col, sum, count, avg, round, datediff, date_format, countDistinct, when

# 1. Define Paths
silver_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/silver/"
gold_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/gold/"

# 2. Read Silver Data
df_orders = spark.read.format("delta").load(f"{silver_path}orders")
df_order_details = spark.read.format("delta").load(f"{silver_path}order_details")
df_items = spark.read.format("delta").load(f"{silver_path}items")
df_addresses = spark.read.format("delta").load(f"{silver_path}addresses")
df_customers = spark.read.format("delta").load(f"{silver_path}customers")

# -------------------------------------------------------------------------
# PRE-CALCULATION: Order Revenue
# (Calculate this once and reuse it for multiple KPIs below)
# -------------------------------------------------------------------------
df_order_revenue = df_order_details.alias("od") \
    .join(df_items.alias("i"), col("od.item_id") == col("i.id")) \
    .groupBy("od.order_id") \
    .agg(sum(col("od.quantity") * col("i.price")).alias("order_revenue"))

# ==========================================
# KPI 1: CUSTOMER AGGREGATIONS (Lifetime Value)
# ==========================================
df_customer_kpis = df_orders.alias("o") \
    .join(df_order_revenue.alias("r"), col("o.id") == col("r.order_id"), "left") \
    .groupBy("o.customer_id") \
    .agg(
        count("o.id").alias("total_lifetime_orders"),
        round(sum("r.order_revenue"), 2).alias("total_lifetime_value"),
        sum("o.coupon_amount").alias("total_coupons_received")
    )

df_customer_kpis_tiered = df_customer_kpis.withColumn(
    "value_tier",
    when(col("total_lifetime_value") >= 3000000, "Platinum ($3M+)")
    .when(col("total_lifetime_value") >= 2000000, "Gold ($2M - $3M)")
    .when(col("total_lifetime_value") >= 1000000, "Silver ($1M - $2M)")
    .otherwise("Bronze (<$1M)")
)

df_customer_kpis_tiered.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}customer_kpis")
print("1. Customer KPI table saved to Gold layer.")

# ==========================================
# KPI 2: REGIONAL AGGREGATIONS (Logistics)
# ==========================================
df_regional_kpis = df_orders.alias("o") \
    .join(df_addresses.alias("a"), col("o.address_id") == col("a.id")) \
    .groupBy("a.state") \
    .agg(
        count("o.id").alias("total_orders_shipped"),
        round(avg(datediff(col("o.delivery_time"), col("o.dispatch_time"))), 1).alias("avg_delivery_time_days")
    )

df_regional_kpis.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}regional_kpis")
print("2. Regional KPI table saved to Gold layer.")

# ==========================================
# KPI 3: PRODUCT AGGREGATIONS (Item Performance)
# ==========================================
df_product_kpis = df_order_details.alias("od") \
    .join(df_items.alias("i"), col("od.item_id") == col("i.id")) \
    .groupBy("i.id", "i.item_name") \
    .agg(
        sum("od.quantity").alias("total_units_sold"),
        round(sum(col("od.quantity") * col("i.price")), 2).alias("total_item_revenue"),
        countDistinct("od.order_id").alias("unique_orders_containing_item")
    )

df_product_kpis.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}product_kpis")
print("3. Product KPI table saved to Gold layer.")

# ==========================================
# KPI 4: MONTHLY SALES TRENDS (Time-Series) 
# ==========================================
df_monthly_kpis = df_orders.alias("o") \
    .join(df_order_revenue.alias("r"), col("o.id") == col("r.order_id"), "left") \
    .withColumn("order_month", date_format(col("o.order_date"), "yyyy-MM")) \
    .groupBy("order_month") \
    .agg(
        count("o.id").alias("total_monthly_orders"),
        round(sum("r.order_revenue"), 2).alias("total_monthly_revenue"),
        sum("o.coupon_amount").alias("total_monthly_coupons_issued")
    )

df_monthly_kpis.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}monthly_kpis")
print("4. Monthly Sales KPI table saved to Gold layer.")