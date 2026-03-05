import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from delta.tables import DeltaTable
from pymongo import MongoClient
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOGGER = logging.getLogger("poc_pipeline")

SCHEMA_NAME = "workspace.agentic_poc"
METADATA_TABLE = f"{SCHEMA_NAME}.metadata_config"
RULES_TABLE = f"{SCHEMA_NAME}.dq_rules"
ISSUES_TABLE = f"{SCHEMA_NAME}.dq_issues_log"
BRONZE_TABLE = f"{SCHEMA_NAME}.bronze_customers"
SILVER_TABLE = f"{SCHEMA_NAME}.silver_customers"


def parse_key_value_args(args: list[str]) -> Dict[str, str]:
    if len(args) % 2 != 0:
        raise ValueError("Arguments must be passed as --key value pairs")

    parsed: Dict[str, str] = {}
    for i in range(0, len(args), 2):
        key = args[i]
        value = args[i + 1]
        if not key.startswith("--"):
            raise ValueError(f"Invalid argument key format: {key}. Expected --key")
        parsed[key[2:]] = value
    return parsed


def get_param(params: Dict[str, str], key: str, env_name: str, default: str = "") -> str:
    return params.get(key) or os.getenv(env_name, default)


def set_runtime_config(
    spark: SparkSession,
    mongodb_uri: str,
    llm_provider: str,
    gemini_api_key: str,
) -> None:
    # Databricks Serverless (Spark Connect) blocks custom spark.conf keys.
    # Keep this function as a no-op so runtime params are still centrally handled.
    _ = spark
    if mongodb_uri:
        LOGGER.info("Runtime mongodb_uri received")
    if llm_provider:
        LOGGER.info("Runtime llm_provider received: %s", llm_provider)
    if gemini_api_key:
        LOGGER.info("Runtime gemini_api_key received")


def setup_metadata(
    spark: SparkSession,
    mongodb_uri: str,
    llm_provider: str,
) -> None:
    if not mongodb_uri.strip():
        raise ValueError("MongoDB URI is required for metadata setup")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

    now = datetime.now(timezone.utc)

    metadata_schema = T.StructType(
        [
            T.StructField("source_name", T.StringType(), False),
            T.StructField("source_format", T.StringType(), False),
            T.StructField("source_options", T.MapType(T.StringType(), T.StringType()), False),
            T.StructField("target_table", T.StringType(), False),
            T.StructField("is_active", T.BooleanType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    metadata_df = spark.createDataFrame(
        [
            (
                "customers_mongo",
                "mongo",
                {
                    "uri": mongodb_uri,
                    "database": "ecommerce",
                    "collection": "customers",
                },
                BRONZE_TABLE,
                True,
                now,
            )
        ],
        schema=metadata_schema,
    )
    metadata_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        METADATA_TABLE
    )

    rules_schema = T.StructType(
        [
            T.StructField("rule_id", T.StringType(), False),
            T.StructField("rule_name", T.StringType(), False),
            T.StructField("rule_sql", T.StringType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("is_active", T.BooleanType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    rules_df = spark.createDataFrame(
        [
            ("R001", "price_positive", "price > 0", "ERROR", True, now),
            ("R002", "email_not_null", "email IS NOT NULL", "ERROR", True, now),
        ],
        schema=rules_schema,
    )
    rules_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(RULES_TABLE)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ISSUES_TABLE} (
          issue_id STRING,
          rule_id STRING,
          rule_name STRING,
          severity STRING,
          failed_condition STRING,
          record_key STRING,
          record_payload STRING,
          timestamp TIMESTAMP,
          pipeline_step STRING
        ) USING DELTA
        """
    )

    LOGGER.info("Metadata setup completed. Schema: %s", SCHEMA_NAME)
    LOGGER.info("LLM provider captured in runtime config: %s", llm_provider)


def _coerce_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            if candidate.endswith("Z"):
                candidate = candidate[:-1] + "+00:00"
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_source_from_mongo(spark: SparkSession, uri: str, database: str, collection: str) -> DataFrame:
    source_schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("full_name", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("price", T.DoubleType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
            T.StructField("_id", T.StringType(), True),
        ]
    )

    with MongoClient(uri) as client:
        docs = list(client[database][collection].find({}))

    if not docs:
        return spark.createDataFrame([], schema=source_schema)

    normalized_docs = []
    for doc in docs:
        normalized_docs.append(
            {
                "customer_id": None if doc.get("customer_id") is None else str(doc.get("customer_id")),
                "full_name": None if doc.get("full_name") is None else str(doc.get("full_name")),
                "email": None if doc.get("email") is None else str(doc.get("email")),
                "price": _coerce_float(doc.get("price")),
                "status": None if doc.get("status") is None else str(doc.get("status")),
                "updated_at": _coerce_timestamp(doc.get("updated_at")),
                "_id": None if doc.get("_id") is None else str(doc.get("_id")),
            }
        )

    return spark.createDataFrame(normalized_docs, schema=source_schema)


def normalize_source(source_df: DataFrame) -> DataFrame:
    return (
        source_df.withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("full_name", F.col("full_name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("updated_at", F.coalesce(F.col("updated_at").cast("timestamp"), F.current_timestamp()))
        .withColumn("source_document_id", F.col("_id").cast("string"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            F.col("customer_id"),
            F.col("full_name"),
            F.col("email"),
            F.col("price"),
            F.col("status"),
            F.col("updated_at"),
            F.col("source_document_id"),
            F.col("ingested_at"),
        )
        .filter(F.col("customer_id").isNotNull())
    )


def ingest_bronze(spark: SparkSession, mongodb_uri_from_args: str) -> None:
    if not spark.catalog.tableExists(METADATA_TABLE):
        raise RuntimeError(f"Metadata table not found: {METADATA_TABLE}")

    metadata_rows = (
        spark.table(METADATA_TABLE)
        .filter((F.col("source_name") == "customers_mongo") & (F.col("is_active") == F.lit(True)))
        .limit(1)
        .collect()
    )
    if not metadata_rows:
        raise RuntimeError("No active metadata row found for source customers_mongo")

    metadata = metadata_rows[0].asDict(recursive=True)
    source_format = metadata.get("source_format", "")
    target_table = metadata.get("target_table", BRONZE_TABLE)
    source_options_raw = metadata.get("source_options") or {}

    effective_mongo_uri = mongodb_uri_from_args or source_options_raw.get("uri", "")
    if not effective_mongo_uri.strip():
        raise ValueError("MongoDB URI is empty in both arguments and metadata table")

    source_options = dict(source_options_raw)
    source_options["uri"] = effective_mongo_uri

    LOGGER.info("Reading source with format '%s' and writing to '%s'", source_format, target_table)

    if source_format == "mongo":
        source_df = _load_source_from_mongo(
            spark=spark,
            uri=source_options["uri"],
            database=source_options.get("database", "ecommerce"),
            collection=source_options.get("collection", "customers"),
        )
    else:
        reader = spark.read.format(source_format)
        for key, value in source_options.items():
            reader = reader.option(key, value)
        source_df = reader.load()

    bronze_df = normalize_source(source_df)
    record_count = bronze_df.count()
    if record_count == 0:
        LOGGER.warning("No records found in source")

    bronze_df.write.format("delta").mode("append").saveAsTable(target_table)
    LOGGER.info("Ingested %s records into %s", record_count, target_table)


def scd2_silver(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
          customer_id STRING,
          full_name STRING,
          email STRING,
          price DOUBLE,
          status STRING,
          source_updated_at TIMESTAMP,
          updated_at TIMESTAMP,
          is_current BOOLEAN,
          valid_from TIMESTAMP,
          valid_to TIMESTAMP,
          record_hash STRING
        ) USING DELTA
        """
    )

    if not spark.catalog.tableExists(BRONZE_TABLE):
        LOGGER.warning("Bronze table does not exist: %s", BRONZE_TABLE)
        return

    bronze_df = spark.table(BRONZE_TABLE)
    if bronze_df.take(1) == []:
        LOGGER.info("Bronze table is empty. Nothing to process.")
        return

    dedup_window = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last(), F.col("ingested_at").desc_nulls_last()
    )
    latest_bronze = (
        bronze_df.withColumn("rn", F.row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("full_name").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("email").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("price").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("status").cast("string"), F.lit("NULL")),
                ),
                256,
            ),
        )
        .withColumn("merge_ts", F.current_timestamp())
    )

    current_silver = spark.table(SILVER_TABLE).filter(F.col("is_current") == F.lit(True))
    delta_table = DeltaTable.forName(spark, SILVER_TABLE)

    changed_current = (
        latest_bronze.alias("s")
        .join(current_silver.alias("t"), on="customer_id", how="inner")
        .filter(F.col("s.record_hash") != F.col("t.record_hash"))
        .select(F.col("s.customer_id"), F.col("s.merge_ts"))
    )
    changed_count = changed_current.count()

    if changed_count > 0:
        (
            delta_table.alias("t")
            .merge(changed_current.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "valid_to": "s.merge_ts",
                    "updated_at": "s.merge_ts",
                }
            )
            .execute()
        )

    upsert_candidates = (
        latest_bronze.alias("s")
        .join(current_silver.alias("t"), on="customer_id", how="left")
        .filter(F.col("t.customer_id").isNull() | (F.col("s.record_hash") != F.col("t.record_hash")))
        .select(
            F.col("s.customer_id").alias("customer_id"),
            F.col("s.full_name").alias("full_name"),
            F.col("s.email").alias("email"),
            F.col("s.price").alias("price"),
            F.col("s.status").alias("status"),
            F.col("s.updated_at").alias("source_updated_at"),
            F.col("s.merge_ts").alias("updated_at"),
            F.lit(True).alias("is_current"),
            F.col("s.merge_ts").alias("valid_from"),
            F.lit(None).cast("timestamp").alias("valid_to"),
            F.col("s.record_hash").alias("record_hash"),
        )
    )
    upsert_count = upsert_candidates.count()

    if upsert_count > 0:
        (
            delta_table.alias("t")
            .merge(upsert_candidates.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
            .whenNotMatchedInsert(
                values={
                    "customer_id": "s.customer_id",
                    "full_name": "s.full_name",
                    "email": "s.email",
                    "price": "s.price",
                    "status": "s.status",
                    "source_updated_at": "s.source_updated_at",
                    "updated_at": "s.updated_at",
                    "is_current": "s.is_current",
                    "valid_from": "s.valid_from",
                    "valid_to": "s.valid_to",
                    "record_hash": "s.record_hash",
                }
            )
            .execute()
        )

    LOGGER.info(
        "SCD Type 2 merge complete. changed_current=%s, inserted_new_versions=%s",
        changed_count,
        upsert_count,
    )


def dq_engine(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ISSUES_TABLE} (
          issue_id STRING,
          rule_id STRING,
          rule_name STRING,
          severity STRING,
          failed_condition STRING,
          record_key STRING,
          record_payload STRING,
          timestamp TIMESTAMP,
          pipeline_step STRING
        ) USING DELTA
        """
    )

    if not spark.catalog.tableExists(SILVER_TABLE):
        raise RuntimeError(f"Silver table does not exist: {SILVER_TABLE}")
    if not spark.catalog.tableExists(RULES_TABLE):
        raise RuntimeError(f"DQ rules table does not exist: {RULES_TABLE}")

    current_silver = spark.table(SILVER_TABLE).filter(F.col("is_current") == F.lit(True))
    if current_silver.take(1) == []:
        LOGGER.info("No current records in Silver. DQ checks skipped.")
        return

    active_rules = spark.table(RULES_TABLE).filter(F.col("is_active") == F.lit(True)).collect()
    if not active_rules:
        LOGGER.warning("No active DQ rules found. DQ checks skipped.")
        return

    issues_buffer: Optional[DataFrame] = None

    for rule_row in active_rules:
        rule_id = rule_row["rule_id"]
        rule_name = rule_row["rule_name"]
        rule_sql = rule_row["rule_sql"]
        severity = rule_row["severity"]

        failed_rows = (
            current_silver.filter(F.expr(f"NOT ({rule_sql}) OR ({rule_sql}) IS NULL"))
            .withColumn("issue_id", F.expr("uuid()"))
            .withColumn("rule_id", F.lit(rule_id))
            .withColumn("rule_name", F.lit(rule_name))
            .withColumn("severity", F.lit(severity))
            .withColumn("failed_condition", F.lit(rule_sql))
            .withColumn("record_key", F.col("customer_id"))
            .withColumn(
                "record_payload",
                F.to_json(
                    F.struct(
                        F.col("customer_id"),
                        F.col("full_name"),
                        F.col("email"),
                        F.col("price"),
                        F.col("status"),
                        F.col("source_updated_at"),
                        F.col("updated_at"),
                    )
                ),
            )
            .withColumn("timestamp", F.current_timestamp())
            .withColumn("pipeline_step", F.lit("DqEngine"))
            .select(
                "issue_id",
                "rule_id",
                "rule_name",
                "severity",
                "failed_condition",
                "record_key",
                "record_payload",
                "timestamp",
                "pipeline_step",
            )
        )

        failed_count = failed_rows.count()
        if failed_count > 0:
            LOGGER.error("DQ rule failed: %s (%s). Failed rows: %s", rule_id, rule_name, failed_count)
            issues_buffer = failed_rows if issues_buffer is None else issues_buffer.unionByName(failed_rows)
        else:
            LOGGER.info("DQ rule passed: %s (%s)", rule_id, rule_name)

    total_failures = issues_buffer.count() if issues_buffer is not None else 0
    if total_failures > 0:
        issues_buffer.write.format("delta").mode("append").saveAsTable(ISSUES_TABLE)
        LOGGER.error("Data quality failed. Logged %s issue rows to %s", total_failures, ISSUES_TABLE)
        raise RuntimeError(f"Data quality checks failed with {total_failures} issues")

    LOGGER.info("Data quality checks completed successfully")


def run_step(
    spark: SparkSession,
    step: str,
    mongodb_uri: str,
    llm_provider: str,
    gemini_api_key: str,
) -> None:
    if step == "SetupMetadata":
        setup_metadata(spark, mongodb_uri=mongodb_uri, llm_provider=llm_provider)
        return
    if step == "IngestBronze":
        ingest_bronze(spark, mongodb_uri_from_args=mongodb_uri)
        return
    if step == "Scd2Silver":
        scd2_silver(spark)
        return
    if step == "DqEngine":
        dq_engine(spark)
        return
    raise ValueError(f"Unsupported pipeline step: {step}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    params = parse_key_value_args(sys.argv[1:])
    step = get_param(params, "step", "PIPELINE_STEP")
    if not step:
        raise ValueError("Pipeline step is required. Use --step <SetupMetadata|IngestBronze|Scd2Silver|DqEngine>.")

    mongodb_uri = get_param(params, "mongodb-uri", "MONGODB_URI")
    llm_provider = get_param(params, "llm-provider", "LLM_PROVIDER", "gemini")
    gemini_api_key = get_param(params, "gemini-api-key", "GEMINI_API_KEY")

    spark = SparkSession.builder.appName(step).getOrCreate()
    try:
        set_runtime_config(spark, mongodb_uri, llm_provider, gemini_api_key)
        run_step(
            spark=spark,
            step=step,
            mongodb_uri=mongodb_uri,
            llm_provider=llm_provider,
            gemini_api_key=gemini_api_key,
        )
    except Exception:
        LOGGER.exception("%s failed", step)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
