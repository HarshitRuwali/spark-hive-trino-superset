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
    base = s3_path.rstrip("/").split("/")[-1]
    name = re.sub(r"\.parquet$", "", base, flags=re.IGNORECASE)
    return sanitize_column(name).lower()


def _sync_to_hive(
    bucket_name: str, folder_name: str, config: dict, logger: Logger
) -> None:
    """Sync the saved parquet files to Hive.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param folder_name: The name of the folder in the S3 bucket.
    :type folder_name: str
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

            ddl = f"""
                CREATE EXTERNAL TABLE
                IF NOT EXISTS {table_name} ({schema_str})
                STORED AS PARQUET LOCATION '{temp_folder_path}'
            """

            try:
                spark.sql(ddl)
                logger.info(
                    f"------ Table `{table_name}` created at {temp_folder_path}\n"
                )
            except Exception as e:
                logger.exception(f"------ Failed to create table `{table_name}`: {e}")

    spark.stop()
    logger.info("Hive sync daemon finished.")


def sync_to_hive(
    bucket_name: str, folder_name: str, config: dict, logger: Logger
) -> None:
    """Main function to sync S3 Parquet files to Hive tables.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param config: The configuration dictionary.
    :type config: dict
    :param logger: The logger instance.
    :type logger: Logger
    """
    _sync_to_hive(bucket_name, folder_name, config, logger)

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

    # sync_to_hive(arags.bucket, args.folder, config, logger)
    folders = [
        "property_data",
        "floor_data",
        "floor_plans_data",
        "seats_data"
    ]
    for folder in folders:
        logger.info(f"Syncing folder: {folder}")
        # Call the sync_to_hive function for each folder
        sync_to_hive("analytics-infinity", folder, config, logger)


if __name__ == "__main__":
    main()
