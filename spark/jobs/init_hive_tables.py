import re

from pyspark import SparkConf
from pyspark.sql import SparkSession


def sanitize_column(name):
    return re.sub(r'\W+', '_', name.strip())


def extract_table_name(s3_path: str) -> str:
    """Derive table name from file path, removing .parquet and sanitizing."""
    base = s3_path.rstrip("/").split("/")[-1]
    name = re.sub(r"\.parquet$", "", base, flags=re.IGNORECASE)
    return sanitize_column(name).lower()


conf = SparkConf().set("spark.eventLog.enabled", "false")

# Create SparkSession with Hive support
spark = SparkSession.builder \
    .appName("CreateHiveTable") \
    .master("spark://spark-master:7077") \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()


# Set your root path (bucket level)
s3_root = "s3a://p2p-analytics/"

# Get HDFS FileSystem from JVM
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(s3_root)
files_status = fs.listStatus(path)

for file_status in files_status:
    full_path = file_status.getPath().toString()
    if full_path.endswith("/"):
        print(f"------ Skipping directory {full_path}")
        continue

    if full_path.endswith(".parquet"):
        table_name = extract_table_name(full_path)
        temp_folder_path = full_path.replace(".parquet", "") + "/"

        print(f"------ File detected: {full_path} → Writing to folder {temp_folder_path}")

        try:
            # Read the single .parquet file
            df = spark.read.parquet(full_path)
            # Write it into a proper Hive-compatible folder
            df.write.mode("overwrite").parquet(temp_folder_path)
        except Exception as e:
            print(f"------ Failed to reprocess file {full_path}: {e}")
            continue

        # Build schema
        seen = set()
        schema_str = ",\n  ".join([
            f"{sanitize_column(f.name)} {f.dataType.simpleString().upper()}"
            for f in df.schema.fields
            if sanitize_column(f.name).lower() not in seen and not seen.add(sanitize_column(f.name).lower())
        ])

        ddl = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
              {schema_str}
            )
            STORED AS PARQUET
            LOCATION '{temp_folder_path}'
        """

        try:
            spark.sql(ddl)
            print(f"------ Table `{table_name}` created at {temp_folder_path}\n")
        except Exception as e:
            print(f"------ Failed to create table `{table_name}`: {e}")

spark.stop()
