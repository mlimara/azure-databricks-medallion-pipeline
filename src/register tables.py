# Databricks notebook source
bronze_path = "abfss://imp-data@imptestwesteurope.dfs.core.windows.net/bronze/"

# 2. Register the tables using their storage locations
spark.sql(f"CREATE TABLE IF NOT EXISTS customers USING DELTA LOCATION '{bronze_path}customers'")
spark.sql(f"CREATE TABLE IF NOT EXISTS addresses USING DELTA LOCATION '{bronze_path}addresses'")
spark.sql(f"CREATE TABLE IF NOT EXISTS items USING DELTA LOCATION '{bronze_path}items'")
spark.sql(f"CREATE TABLE IF NOT EXISTS orders USING DELTA LOCATION '{bronze_path}orders'")
spark.sql(f"CREATE TABLE IF NOT EXISTS order_details USING DELTA LOCATION '{bronze_path}order_details'")

print("Tables successfully registered in the Metastore!")