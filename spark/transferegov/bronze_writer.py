"""
Módulo de gravação tabular fiel na camada Bronze com Spark e Delta Lake.
"""
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from transferegov.config import AppConfig, DatasetConfig

logger = logging.getLogger(__name__)

def get_spark_session(config: AppConfig) -> SparkSession:
    """
    Cria ou recupera a SparkSession conectada ao cluster existente,
    com recursos conservadores e suporte a Delta Lake e MinIO S3A.
    """
    builder = (
        SparkSession.builder
        .appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", config.storage.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", config.storage.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.storage.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.executor.memory", config.spark.executor_memory)
        .config("spark.executor.cores", str(config.spark.executor_cores))
        .config("spark.cores.max", str(config.spark.cores_max))
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()

def write_bronze_delta_table(
    spark: SparkSession,
    csv_path: str,
    header_columns: List[str],
    dataset_cfg: DatasetConfig,
    run_id: str,
    source_file: str,
    source_sha256: str,
    logical_row_count: int
) -> Dict[str, Any]:
    """
    Lê o CSV através do Spark com tipagem estrita StringType para todas as colunas
    oficiais, anexa os metadados técnicos de auditoria e grava a tabela Delta
    em s3a://bronze/warehouse/<dataset> via overwrite transacional.

    Reconcilia a contagem lógica independente com a contagem física gravada no Delta.
    """
    logger.info(f"Iniciando gravação Delta Bronze para {dataset_cfg.table_name} a partir de {csv_path}...")

    # Schema estrito: 100% StringType para campos oficiais (preserva zeros à esquerda e texto exato)
    schema_fields = [StructField(col_name, StringType(), True) for col_name in header_columns]
    strict_schema = StructType(schema_fields)

    # Leitura fiel com configurações explícitas
    df = (
        spark.read
        .format("csv")
        .schema(strict_schema)
        .option("header", "true")
        .option("delimiter", ";")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .option("emptyValue", "")
        .load(csv_path)
    )

    # Inclusão exclusiva das 4 colunas técnicas de auditoria
    now_utc = datetime.now(timezone.utc)
    df_with_meta = (
        df
        .withColumn("__ingested_at_utc", F.lit(now_utc.isoformat()).cast(StringType()))
        .withColumn("__ingestion_run_id", F.lit(run_id).cast(StringType()))
        .withColumn("__source_file", F.lit(source_file).cast(StringType()))
        .withColumn("__source_sha256", F.lit(source_sha256).cast(StringType()))
    )

    # Gravação transacional overwrite no Delta Lake
    logger.info(f"Gravando tabela Delta em {dataset_cfg.delta_path}...")
    (
        df_with_meta.write
        .format("delta")
        .mode("overwrite")
        .save(dataset_cfg.delta_path)
    )

    # Leitura e reconciliação a partir da tabela Delta efetivamente gravada
    delta_written = spark.read.format("delta").load(dataset_cfg.delta_path)
    delta_output_rows = delta_written.count()

    logger.info(
        f"Reconciliação de registros para {dataset_cfg.table_name}: "
        f"CSV lógico={logical_row_count}, Delta gravado={delta_output_rows}"
    )

    if delta_output_rows != logical_row_count:
        raise ValueError(
            f"Falha de reconciliação para {dataset_cfg.table_name}: "
            f"a contagem lógica de entrada ({logical_row_count}) diverge "
            f"dos registros gravados no Delta ({delta_output_rows})."
        )

    # Obter a versão do commit Delta
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{dataset_cfg.delta_path}` LIMIT 1")
    delta_version = int(history_df.collect()[0]["version"])

    logger.info(f"Tabela Delta {dataset_cfg.table_name} gravada com sucesso na versão {delta_version}.")

    return {
        "dataset_id": dataset_cfg.id,
        "delta_path": dataset_cfg.delta_path,
        "delta_version": delta_version,
        "delta_output_rows": delta_output_rows,
        "logical_input_rows": logical_row_count,
        "columns": header_columns,
        "ingested_at_utc": now_utc.isoformat()
    }
