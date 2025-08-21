"""Sync S3 Parquet files to Hive tables daemon"""

import re
import time
from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from trino.auth import BasicAuthentication

# ----------------------------
# Trino client (optional)
# ----------------------------
def _get_trino_conn(cfg, logger: Logger):
    if not cfg.get("enabled", False):
        return None
    try:
        import trino
        auth = None
        if cfg.get("user") and cfg.get("password"):
            # Basic auth
            auth = BasicAuthentication(cfg["user"], cfg["password"])
        conn = trino.dbapi.connect(
            host=cfg.get("host", "trino"),
            port=int(cfg.get("port", 8080)),
            user=cfg.get("user", "spark-job"),
            http_scheme=cfg.get("http_scheme", "http"),
            catalog=cfg.get("catalog", "hive"),
            schema=cfg.get("schema", "default"),
            auth=auth,
            verify=cfg.get("verify_ssl", False),
        )
        return conn
    except Exception as e:
        logger.warning(f"Trino client not available: {e}")
        return None


def create_or_replace_latest_view_trino(
    trino_conn, catalog: str, database: str, table_name: str, logger: Logger
):
    """
    Create/replace a Trino view <table>_latest that selects only the newest partition,
    using <table>$partitions to avoid scanning data files.
    """
    view_fqn = f'{catalog}.{database}.{table_name}_latest'
    table_fqn = f'{catalog}.{database}.{table_name}'
    partitions_fqn = f'{catalog}.{database}."{table_name}$partitions"'
    ddl = f"""
        CREATE OR REPLACE VIEW {view_fqn} AS
        SELECT *
        FROM {table_fqn}
        WHERE snapshot_date = (
            SELECT max(snapshot_date) FROM {partitions_fqn}
        )
    """
    try:
        cur = trino_conn.cursor()
        cur.execute(ddl)
        cur.fetchall()  # consume
        logger.info(f"Created/updated Trino view: {view_fqn}")
    except Exception as e:
        logger.warning(f"Failed to create Trino latest view for {table_fqn}: {e}")

# ----------------------------
# Utilities
# ----------------------------

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


def detect_snapshot_col(df):
    """Return a column name in df that can serve as snapshot_date, else None."""
    cols_lower = [c.lower() for c in df.columns]
    candidate_cols = ["snapshot_date"]
    for c in candidate_cols:
        if c in cols_lower:
            return df.columns[cols_lower.index(c)]
    return None


def ensure_snapshot_date(df, full_path: str):
    """
    Ensure a DATE-typed column named 'snapshot_date' exists.
    If not present, infer from YYYY-MM-DD in path.
    Returns: (schema, df_with_snapshot_date, inferred_date_str_or_None)
    """
    col = detect_snapshot_col(df)
    inferred = None
    date_from_path_re = re.compile(r"(\d{4}-\d{2}-\d{2})")
    if col:
        if col != "snapshot_date":
            df = df.withColumn("snapshot_date", F.col(col))
        df = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
    else:
        m = date_from_path_re.search(full_path)
        if not m:
            raise ValueError(
                "Could not find snapshot date column in data or infer from filename."
            )
        inferred = m.group(1)
        df = df.withColumn("snapshot_date", F.lit(inferred).cast(T.DateType()))

    # Final cast/sanity
    df = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date")))
    return df.schema, df, inferred


def build_partitioned_table(spark, database, table_name, df_schema, table_root_path):
    """
    Create (if not exists) an external, partitioned Hive table with PARTITIONED BY (snapshot_date DATE).
    Excludes 'snapshot_date' from the base column list and forces valid_to DATE if present.
    """
    seen = set()
    cols = []
    for f in df_schema.fields:
        col_name = sanitize_column(f.name).lower()
        if col_name in seen:
            continue
        seen.add(col_name)
        if col_name == "snapshot_date":
            # Goes into PARTITIONED BY, not base columns
            continue
        spark_type = f.dataType.simpleString().upper()
        cols.append(f"{col_name} {spark_type}")

    schema_str = ",\n  ".join(cols) if cols else ""
    # Force valid_to to DATE if present
    schema_str = re.sub(
        r"\bvalid_to\b\s+[A-Z0-9()]+", "valid_to DATE", schema_str, flags=re.IGNORECASE
    )

    full_table_name = f"{database}.{table_name}"
    ddl = f"""
        CREATE DATABASE IF NOT EXISTS {database};
        CREATE EXTERNAL TABLE IF NOT EXISTS {full_table_name} (
          {schema_str}
        )
        PARTITIONED BY (snapshot_date DATE)
        STORED AS PARQUET
        LOCATION '{table_root_path}';
    """

    for stmt in ddl.strip().split(";"):
        s = stmt.strip()
        if s:
            spark.sql(s)

    # Sync partitions that might already exist
    spark.sql(f"MSCK REPAIR TABLE {full_table_name}")
    return full_table_name


def build_nonpartitioned_table(spark, database, table_name, df_schema, table_location):
    """Create/replace a non-partitioned external Hive table at a specific location."""
    seen = set()
    schema_str = ",\n  ".join(
        [
            f"{sanitize_column(f.name)} {f.dataType.simpleString().upper()}"
            for f in df_schema.fields
            if sanitize_column(f.name).lower() not in seen
            and not seen.add(sanitize_column(f.name).lower())
        ]
    )
    # Force valid_to to DATE if present
    schema_str = re.sub(
        r"\bvalid_to\b\s+[A-Z0-9()]+", "valid_to DATE", schema_str, flags=re.IGNORECASE
    )

    full_table_name = f"{database}.{table_name}"
    ddl = f"""
        DROP TABLE IF EXISTS {full_table_name};
        CREATE EXTERNAL TABLE IF NOT EXISTS {full_table_name} (
          {schema_str}
        )
        STORED AS PARQUET
        LOCATION '{table_location}';
    """
    for stmt in ddl.strip().split(";"):
        s = stmt.strip()
        if s:
            spark.sql(s)
    return full_table_name


# ----------------------------
# Main sync logic
# ----------------------------

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

    # Spark configuration
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
        .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")  # Dynamic partitioning settings (used for FACT_* writes)
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
    )

    conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    conf.set("spark.hadoop.fs.s3a.threads.max", "100")
    conf.set("spark.hadoop.fs.s3a.connection.timeout", "5000")
    conf.set("spark.hadoop.fs.s3a.retry.limit", "5")
    conf.set("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
    conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    # Create SparkSession with Hive support
    spark = (
        SparkSession.builder.appName(spark_app_name)
        .master(f"spark://{spark_master_host}:{spark_master_port}")
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Ensure schema exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    logger.info(f"Ensured Hive schema (database) '{database}' exists.")

    s3_root = f"s3a://{bucket_name}/{folder_name}/latest/"

    # Use Hadoop FS via JVM to list 'latest' files
    uri = spark._jvm.java.net.URI
    _path = spark._jvm.org.apache.hadoop.fs.Path
    file_system = spark._jvm.org.apache.hadoop.fs.FileSystem

    path = _path(s3_root)
    fs = file_system.get(uri(s3_root), spark._jsc.hadoopConfiguration())
    files_status = fs.listStatus(path)

    is_fact = folder_name.upper().startswith("FACT_")

    # Prepare optional Trino connection (once)
    trino_cfg = config.get("trino", {})
    trino_conn = _get_trino_conn(trino_cfg, logger)
    trino_catalog = trino_cfg.get("catalog", "hive")

    for file_status in files_status:
        full_path = file_status.getPath().toString()
        if full_path.endswith("/"):
            logger.info(f"------ Skipping directory {full_path}")
            continue

        if not full_path.endswith(".parquet"):
            logger.info(f"------ Skipping non-parquet {full_path}")
            continue

        table_name = extract_table_name(full_path)

        if is_fact:
            # ---------- Partitioned path & table for FACT_* ----------
            table_root_path = f"s3a://{bucket_name}/{folder_name}/partitioned/"
            logger.info(
                f"------ FACT detected: {full_path} -> writing partitioned to {table_root_path}"
            )
            try:
                start = time.time()
                df = spark.read.parquet(full_path)
                logger.info(f"-------- Read parquet in {time.time() - start:.2f}s")

                # Ensure snapshot_date column
                df_schema, df, inferred_date = ensure_snapshot_date(df, full_path)

                # Create partitioned table if needed
                full_table_name = build_partitioned_table(
                    spark=spark,
                    database=database,
                    table_name=table_name,
                    df_schema=df_schema,
                    table_root_path=table_root_path,
                )

                # If file has multiple dates (rare), you can keep all; if you expect one date per file, filter:
                if inferred_date:
                    df = df.filter(F.col("snapshot_date") == F.lit(inferred_date).cast("date"))

                # Append data into snapshot_date partition
                (
                    df.write
                    .mode("append")
                    .partitionBy("snapshot_date")
                    .parquet(table_root_path)
                )

                # Discover new partitions & compute stats
                spark.sql(f"MSCK REPAIR TABLE {full_table_name}")
                spark.sql(
                    f"ANALYZE TABLE {full_table_name} PARTITION (snapshot_date) COMPUTE STATISTICS"
                )

                logger.info(f"------ Table `{full_table_name}` updated at {table_root_path}\n")

                # -----------------------------
                # NEW: Create/refresh Trino <table>_latest view (migration path)
                # -----------------------------
                if trino_conn:
                    msg = f"----- Creating Trino latest view for {table_name}..."
                    logger.info(msg)
                    try:
                        create_or_replace_latest_view_trino(
                            trino_conn=trino_conn,
                            catalog=trino_catalog,
                            database=database,
                            table_name=table_name,
                            logger=logger,
                        )
                    except Exception as e:
                        logger.warning(f"Could not create Trino latest view for {table_name}: {e}")
                else:
                    logger.info(
                        f"Trino not configured; skipping latest view creation for {table_name}."
                    )

            except Exception as e:
                logger.exception(f"------ Failed processing FACT file {full_path}: {e}")
                continue

        else:
            # ---------- Non-partitioned path & table for DIM_* ----------
            temp_folder_path = full_path.replace(".parquet", "") + "/"
            logger.info(
                f"------ DIM detected: {full_path} -> writing to folder {temp_folder_path}"
            )

            try:
                start = time.time()
                df = spark.read.parquet(full_path)
                logger.info(f"-------- Read parquet in {time.time() - start:.2f}s")

                # Write it into a proper Hive-compatible folder
                df.write.mode("overwrite").parquet(temp_folder_path)

                # Register/replace external table (non-partitioned)
                full_table_name = build_nonpartitioned_table(
                    spark=spark,
                    database=database,
                    table_name=table_name,
                    df_schema=df.schema,
                    table_location=temp_folder_path,
                )

                logger.info(
                    f"------ Table `{full_table_name}` created at {temp_folder_path}\n"
                )

            except Exception as e:
                logger.exception(f"------ Failed to process DIM file {full_path}: {e}")
                continue

    # Close Trino connection if opened
    try:
        if trino_conn:
            trino_conn.close()
    except Exception:
        pass

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
            "secret_key": "",
        },
        "trino": {
            "enabled": True,
            "host": "",
            "port": 8443,
            "http_scheme": "https",
            "user": "",
            "password": "",
            "verify_ssl": False,
            "catalog": "hive",
            "schema": "infinity",
        }
    }

    database = "infinity"
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
