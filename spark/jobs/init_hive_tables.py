import re

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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
    .appName("CreateOrMergeHiveTable") \
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

        print(f"------ File detected: {full_path} -> Writing to folder {temp_folder_path}")

        try:
            # Read the single .parquet file
            df_new = spark.read.parquet(full_path)
            df_new = df_new.dropDuplicates(["id"])
        except Exception as e:
            print(f"------ Failed to read file {full_path}: {e}")
            continue

        try:
            df_existing = spark.table(table_name)
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            print(f"------ Merging into existing table `{table_name}`")

            # Get list of IDs in new data
            new_ids = [row["id"] for row in df_new.select("id").distinct().collect()]
            df_to_keep = df_existing.filter(~col("id").isin(new_ids))

            # Merge
            df_merged = df_to_keep.unionByName(df_new)

            # Overwrite
            df_merged.write.mode("overwrite").format("hive").saveAsTable(table_name)

        else:
            print(f"------ Creating new table `{table_name}`")

            # Write it into a proper Hive-compatible folder
            try:
                df_new.write.mode("overwrite").parquet(temp_folder_path)
            except Exception as e:
                print(f"------ Failed to write to temp folder {temp_folder_path}: {e}")
                continue

            # Build schema
            seen = set()
            schema_str = ",\n  ".join([
                f"{sanitize_column(f.name)} {f.dataType.simpleString().upper()}"
                for f in df_new.schema.fields
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
