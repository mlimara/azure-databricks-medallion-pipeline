# Databricks notebook source
# task 5 - Addresses
#Creating silver layer for addresses
from pyspark.sql.functions import col, trim, regexp_replace, initcap, regexp_extract, lower, upper

# 1. Paths
bronze_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/bronze/"
silver_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/silver/"
dlq_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/dlq/"
zip_csv_path = "/Volumes/imp_test_team_c/lemara_mukhamedyarova_bronze/volume/uszips.csv"

# 2. Read Data
df_bronze_addresses = spark.read.format("delta").load(f"{bronze_path}addresses")
df_zip_lookup = spark.read.format("csv").option("header", "true").load(zip_csv_path)

# 3. PREPARE THE LOOKUP TABLE
df_zip_clean = df_zip_lookup.select(
    col("zip").alias("postal_code_lookup"),
    regexp_replace(lower(col("city")), r"[^a-z]", "").alias("lookup_city_key"),
    regexp_replace(lower(col("state_name")), r"[^a-z]", "").alias("lookup_state_key")
).dropDuplicates(["lookup_city_key", "lookup_state_key"])

# 4. RESTRUCTURE THE ADDRESS TABLE
df_restructured = df_bronze_addresses \
    .withColumn("house_number", regexp_extract(col("addressline"), r"^[^\d]*(\d+)", 1).cast("long")) \
    .withColumn("street", trim(regexp_replace(col("addressline"), r"^[^\d]*\d+[\s\.,]*", ""))) \
    .withColumn("city", trim(regexp_replace(col("city"), r"[^a-zA-Z \-]", " "))) \
    .withColumn("state", trim(regexp_replace(col("state"), r"[^a-zA-Z \-]", " "))) \
    .withColumnRenamed("createdOn", "created_on") \
    .drop("addressline")

# Target 1 or more spaces (r" +") and collapse them into a single space
def cleanse_string_col(column_name):
    if column_name == "country":
        return upper(trim(regexp_replace(col(column_name), r"\s+", " ")))
    else:
        return initcap(trim(regexp_replace(col(column_name), r"\s+", " ")))

df_cleaned = df_restructured \
    .withColumn("street", cleanse_string_col("street")) \
    .withColumn("city", cleanse_string_col("city")) \
    .withColumn("state", cleanse_string_col("state")) \
    .withColumn("country", cleanse_string_col("country"))

# -----------------------------------------------------------------
# 5. THE BULLETPROOF JOIN
# -----------------------------------------------------------------
df_joined = df_cleaned \
    .withColumn("join_city_key", regexp_replace(lower(col("city")), r"[^a-z]", "")) \
    .withColumn("join_state_key", regexp_replace(lower(col("state")), r"[^a-z]", "")) \
    .join(
        df_zip_clean, 
        (col("join_city_key") == col("lookup_city_key")) & (col("join_state_key") == col("lookup_state_key")), 
        "left"
    ) \
    .withColumnRenamed("postal_code_lookup", "postal_code") \
    .drop("join_city_key", "join_state_key", "lookup_city_key", "lookup_state_key")

# -----------------------------------------------------------------
# 6. VALIDATION & ROUTING (The DLQ Filter)
# -----------------------------------------------------------------
invalid_char_regex = r"[^a-zA-Z0-9 \.,\-]"

df_validated = df_joined.withColumn(
    "is_invalid",
    col("street").rlike(invalid_char_regex) |
    col("city").rlike(invalid_char_regex) |
    col("house_number").isNull()
)

df_valid_addresses = df_validated.filter(col("is_invalid") == False).drop("is_invalid")
df_valid_addresses.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}addresses")

df_invalid_addresses = df_validated.filter(col("is_invalid") == True)
df_invalid_addresses.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{dlq_path}addresses")

print("--- Task 5: Final Cleansing & Join Results ---")
print(f"Total Addresses Processed: {df_bronze_addresses.count()}")
print(f"Valid Addresses routed to Silver: {df_valid_addresses.count()}")
print(f"Invalid Addresses routed to DLQ: {df_invalid_addresses.count()}")

# COMMAND ----------

# task 5 - Customers, Items, Order, Order Details tables

from pyspark.sql.functions import col, when, lit, monotonically_increasing_id, sum, lower
from pyspark.sql.types import DecimalType

# 1. Paths
bronze_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/bronze/"
silver_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/silver/"
dlq_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/data/Team_C/lemara_mukhamedyarova/dlq/"
# Define the complete list of US Capitals 
us_capitals = [
    "washington", "montgomery", "juneau", "phoenix", "little rock", "sacramento", 
    "denver", "hartford", "dover", "tallahassee", "atlanta", "honolulu", "boise", 
    "springfield", "indianapolis", "des moines", "topeka", "frankfort", "baton rouge", 
    "augusta", "annapolis", "boston", "lansing", "st paul", "jackson", "jefferson city", 
    "helena", "lincoln", "carson city", "concord", "trenton", "santa fe", "albany", 
    "raleigh", "bismarck", "columbus", "oklahoma city", "salem", "harrisburg", 
    "providence", "columbia", "pierre", "nashville", "austin", "salt lake city", 
    "montpelier", "richmond", "olympia", "charleston", "madison", "cheyenne"
]
df_silver_addresses = spark.read.format("delta").load(f"{silver_path}addresses") # to check city

# ==========================================
# 2. PROCESS CUSTOMERS TABLE
# ==========================================
df_bronze_customers = spark.read.format("delta").load(f"{bronze_path}customers")

df_silver_customers = df_bronze_customers \
    .withColumn("customer_kind", col("type")) \
    .withColumn(
        "customer_type", 
        # ENFORCING THE RULE: If they are an affiliate, force them to be 'regular'
        when(lower(col("type")).contains("affiliate"), lit("regular"))
        # Otherwise, keep whatever status they originally had (VIP or regular)
        .otherwise(col("status")) 
    ) \
    .withColumn("created_on", col("CreatedOn")) \
    .withColumn("id", col("id").cast("long")) \
    .drop("status", "type", "CreatedOn")
df_silver_customers.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}customers")
print("Customers table saved to Silver.")

# ==========================================
# 3. PROCESS ITEMS TABLE
# ==========================================
df_bronze_items = spark.read.format("delta").load(f"{bronze_path}items")

df_silver_items = df_bronze_items \
    .withColumn("item_name", col("Descriptions")) \
    .withColumn("description", col("Codes")) \
    .withColumn("id", col("id").cast("long")) \
    .withColumn("price", col("price").cast(DecimalType(10,2))) \
    .drop("Description", "Codes") # Dropping the legacy bronze columns

df_silver_items.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}items")
print("Items table saved to Silver.")

# ==========================================
# 4. PROCESS ORDER DETAILS TABLE
# ==========================================
df_bronze_order_details = spark.read.format("delta").load(f"{bronze_path}order_details")
order_details = df_bronze_order_details \
    .groupBy("OrderId", "ItemId") \
    .agg(sum(col("Quantity")).alias("Quantity"))
spark.conf.set("spark.sql.caseSensitive", "true")
df_silver_order_details = order_details \
    .withColumn("order_id", col("OrderId").cast("long")) \
    .withColumn("item_id", col("ItemId").cast("long")) \
    .withColumn("quantity", col("Quantity").cast("long")) \
    .withColumn("id", monotonically_increasing_id()) \
    .drop("OrderId", "ItemId", "Quantity") # Dropping the legacy bronze columns

df_silver_order_details.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}order_details")
print("Order Details table saved to Silver.")

# ==========================================
# 5. PROCESS ORDERS TABLE (With Business Logic)
# ==========================================
df_bronze_orders = spark.read.format("delta").load(f"{bronze_path}orders")

df_orders_formatted = df_bronze_orders \
    .withColumn("customer_id", col("customerId").cast("long")) \
    .withColumn("address_id", col("addressId").cast("long")) \
    .withColumn("created_on", col("createdOn").cast("timestamp")) \
    .withColumn("order_date", col("createdOn").cast("date")) \
    .withColumn("dispatch_time", col("deliveryDate").cast("timestamp")) \
    .withColumn("delivery_time", col("deliveredOn").cast("timestamp")) \
    .withColumn("id", col("id").cast("long")) \
    .withColumn("order_type", lit("Regional")) \
    .drop("customerId", "addressId", "createdOn", "deliveryDate", "deliveredOn")

# VALIDATION: Check for chronological impossibilities
df_orders_validated = df_orders_formatted.withColumn(
    "is_invalid",
    # Invalid if dispatch is before creation, OR delivery is before dispatch
    (col("dispatch_time") < col("created_on")) | 
    (col("delivery_time") < col("dispatch_time")) |
    col("dispatch_time").isNull() | 
    col("delivery_time").isNull()
)

# 5. BUSINESS LOGIC: Determine Order Type & Coupon Amount
df_orders_enriched = df_orders_validated.alias("o") \
    .join(df_silver_addresses.alias("a"), col("o.address_id") == col("a.id"), "left") \
    .join(df_silver_customers.alias("c"), col("o.customer_id") == col("c.id"), "left") \
    .withColumn(
        "order_type",
        # Check if the clean address city exists in our capitals list
        when(lower(col("a.city")).isin(us_capitals), lit("Metropolitan")).otherwise(lit("Regional"))
    ) \
    .withColumn(
        "coupon_amount", 
        # Apply $5 ONLY if VIP and Metropolitan
        when((lower(col("c.customer_type")) == "vip") & (col("order_type") == "Metropolitan"), 5).otherwise(0).cast("long")
    ) \
    .select(
        "o.id", "o.customer_id", "o.address_id", "order_type", 
        "o.order_date", "o.dispatch_time", "o.delivery_time", 
        "coupon_amount", "o.created_on", "o.is_invalid"
    )

# Valid orders go to Silver
df_valid_orders = df_orders_enriched.filter(col("is_invalid") == False).drop("is_invalid")
df_valid_orders.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{silver_path}orders")

# Time-traveling (Invalid) orders go to DLQ
df_invalid_orders = df_orders_enriched.filter(col("is_invalid") == True).drop("is_invalid")
df_invalid_orders.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{dlq_path}orders")