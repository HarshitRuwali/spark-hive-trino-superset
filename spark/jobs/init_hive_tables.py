import re

from pyspark import SparkConf
from pyspark.sql import SparkSession

def sanitize_column(name):
    return re.sub(r'\W+', '_', name.strip())

# Disable event logging
conf = SparkConf().set("spark.eventLog.enabled", "false")

# Create SparkSession with Hive support
spark = SparkSession.builder \
    .appName("CreateHiveTable") \
    .master("spark://spark-master:7077") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

# Set parameters
table_name = "analytics"
s3_path = "s3a://p2p-analytics/"

# Read Parquet schema from S3
print(f"Reading schema from: {s3_path}")
df = spark.read.parquet(s3_path)

# Build schema string
schema_str = ",\n  ".join([
    f"{sanitize_column(field.name)} {field.dataType.simpleString().upper()}"
    for field in df.schema.fields
])

# Build dynamic DDL
ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({schema_str})
    STORED AS PARQUET
    LOCATION '{s3_path}'
"""

print("Generated DDL:")
print(ddl)

# Create table
spark.sql(ddl)

print("External Hive table created successfully.")
spark.stop()
