"""Sync S3 Parquet files to Hive tables daemon"""

import re
import time
from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession


def sanitize_column(name: str) -> str:
    """Sanitize column names by replacing non-alphanumeric characters with underscores.

    :param name: The column name to sanitize.
    :type name: str
    :return: The sanitized column name.
    :rtype: str
    """
    return re.sub(r"\W+", "_", name.strip())


def extract_table_name(s3_path: str) -> str:
    """Derive table name from file path, removing .parquet and sanitizing.

    :param s3_path: The S3 path to the file.
    :type s3_path: str
    :return: The sanitized table name.
    :rtype: str
    """
    # Always use the folder name after the bucket as the table name
    # Example: s3://analytics-infinity/onboarding_forms_data/latest/latest.parquet -> onboarding_forms_data
    s3_path = s3_path.rstrip("/")
    parts = s3_path.split("/")
    # Find the index of the bucket name (after 's3://' or 's3a://')
    for i, part in enumerate(parts):
        if part.endswith(".amazonaws.com") or part.startswith("s3") or part == "":
            continue
        bucket_index = i
        break
    # The table name is the next part after the bucket
    table_name = parts[bucket_index + 1] if len(parts) > bucket_index + 1 else parts[-1]
    return sanitize_column(table_name).lower()


def _sync_to_hive(
    bucket_name: str, folder_name: str, database: str, config: dict, logger: Logger
) -> None:
    """Sync the saved parquet files to Hive.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param folder_name: The name of the folder in the S3 bucket.
    :type folder_name: str
    :param database: The name of the Hive database to use.
    :type database: str
    :param config: The config.
    :type config: dict
    :param logger: The logger.
    :type logger: Logger
    """
    logger.info("Starting Hive sync daemon...")
    spark_master_host = config["spark"]["host"]
    spark_master_port = config["spark"]["port"]
    spark_app_name = config["spark"]["app_name"]
    # Disable event logging
    conf = (
        SparkConf()
        .set("spark.eventLog.enabled", "false")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.access.key", config["aws"]["access_key"])
        .set("spark.hadoop.fs.s3a.secret.key", config["aws"]["secret_key"])
        .set("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        .set("spark.sql.warehouse.dir", "/opt/bitnami/spark/spark-warehouse")
        .set("spark.sql.legacy.parquet.nanosAsLong", "true")
    )

    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    conf.set("spark.hadoop.fs.s3a.threads.max", "100")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "5000")
    conf.set("spark.hadoop.fs.s3a.retry.limit", "5")
    conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder.appName(spark_app_name)
        .master(f"spark://{spark_master_host}:{spark_master_port}")
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    # Create the database if it does not exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    logger.info(f"Ensured Hive schema (database) '{database}' exists.")

    s3_root = f"s3a://{bucket_name}/{folder_name}/latest/"
    # Get HDFS FileSystem from JVM
    uri = spark._jvm.java.net.URI
    _path = spark._jvm.org.apache.hadoop.fs.Path
    file_system = spark._jvm.org.apache.hadoop.fs.FileSystem

    path = _path(s3_root)
    fs = file_system.get(uri(s3_root), spark._jsc.hadoopConfiguration())
    files_status = fs.listStatus(path)

    for file_status in files_status:
        full_path = file_status.getPath().toString()
        if full_path.endswith("/"):
            logger.info(f"------ Skipping directory {full_path}")
            continue

        if full_path.endswith(".parquet"):
            table_name = extract_table_name(full_path)
            temp_folder_path = full_path.replace(".parquet", "") + "/"

            logger.info(
                f"------ File detected: {full_path} -> Writing to folder {temp_folder_path}"
            )

            try:
                # Read the single .parquet file
                start = time.time()
                df = spark.read.parquet(full_path)
                logger.info(
                    f"--------_+++++++______________Read parquet in {time.time() - start:.2f}s"
                )
                # df = spark.read.parquet(full_path)
                # Write it into a proper Hive-compatible folder
                df.write.mode("overwrite").parquet(temp_folder_path)
            except Exception as e:
                logger.exception(f"------ Failed to reprocess file {full_path}: {e}")
                continue

            # schema
            seen = set()
            schema_str = ",\n  ".join(
                [
                    f"{sanitize_column(f.name)} {f.dataType.simpleString().upper()}"
                    for f in df.schema.fields
                    if sanitize_column(f.name).lower() not in seen
                    and not seen.add(sanitize_column(f.name).lower())
                ]
            )
            full_table_name = f"{database}.{table_name}"
            ddl = f"""
                DROP TABLE IF EXISTS {full_table_name};
                CREATE EXTERNAL TABLE
                IF NOT EXISTS {full_table_name} ({schema_str})
                STORED AS PARQUET LOCATION '{temp_folder_path}'
            """

            try:
                # spark.sql(ddl)
                for stmt in ddl.strip().split(";"):
                    if stmt.strip():
                        spark.sql(stmt.strip())
                logger.info(
                    f"------ Table `{full_table_name}` created at {temp_folder_path}\n"
                )
            except Exception as e:
                logger.exception(f"------ Failed to create table `{full_table_name}`: {e}")

    spark.stop()
    logger.info("Hive sync daemon finished.")


def sync_to_hive(
    bucket_name: str, folder_name: str, database: str, config: dict, logger: Logger
) -> None:
    """Main function to sync S3 Parquet files to Hive tables.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param folder_name: The name of the folder in the S3 bucket.
    :type folder_name: str
    :param database: The name of the Hive database to use.
    :type database: str
    :param config: The configuration dictionary.
    :type config: dict
    :param logger: The logger instance.
    :type logger: Logger
    """
    _sync_to_hive(bucket_name, folder_name, database, config, logger)

def main():
    """Main function to run the sync_to_hive script."""
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Sync S3 Parquet files to Hive tables.")
    # parser.add_argument("--bucket", required=True, help="S3 bucket name")
    # parser.add_argument("--folder", required=True, help="Folder name in the S3 bucket")
    # args = parser.parse_args()

    # Configure logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Example config dictionary
    config = {
        "spark": {
            "host": "spark-master",
            "port": "7077",
            "app_name": "HiveSyncApp"
        },
        "aws": {
            "access_key": "",
            "secret_key": ""
        }
    }
    database = "infinity"
    # sync_to_hive(arags.bucket, args.folder, config, logger)
    folders = [
       "DIM_BLOCKING_REQUESTS",
        "DIM_COMPANIES",
        "DIM_CONTACTS",
        "DIM_CONTRACT_BREAKDOWNS",
        "DIM_DATE",
        "DIM_FLOORS",
        "DIM_HUBSPOT_DEALS",
        "DIM_PRODUCT_TYPES",
        "DIM_PROPERTIES",
        "DIM_SEATS",
        "DIM_ZONES",
        "FACT_BLOCKING_REQUESTS",
        "FACT_CONTRACTS",
        "FACT_SEAT_OCCUPANCY",
    ]
    for folder in folders:
        logger.info(f"Syncing folder: {folder}")
        # Call the sync_to_hive function for each folder
        sync_to_hive("analytics-infinity", folder, database, config, logger)


if __name__ == "__main__":
    main()
