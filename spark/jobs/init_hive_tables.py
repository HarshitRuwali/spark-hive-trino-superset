from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InitHiveTables") \
    .master("spark://spark-master:7077") \
    .enableHiveSupport() \
    .getOrCreate()

# Replace with your actual schema
df = spark.read.parquet("s3a://p2p-analytics/analytics/")
df.createOrReplaceTempView("temp_table")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS analytics (
  id INT,
  name STRING,
  created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3a://p2p-analytics/analytics/'
""")

spark.stop()
