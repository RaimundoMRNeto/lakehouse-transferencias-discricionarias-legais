"""
Módulo de auditoria persistente da camada Bronze.
Gerencia as tabelas Delta bronze.ingestion_runs e bronze.ingestion_manifest.
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    IntegerType, LongType, DoubleType
)

logger = logging.getLogger(__name__)

RUNS_SCHEMA = StructType([
    StructField("ingestion_run_id", StringType(), False),
    StructField("start_time_utc", StringType(), False),
    StructField("end_time_utc", StringType(), True),
    StructField("status", StringType(), False),
    StructField("force", BooleanType(), False),
    StructField("source_data_carga_raw_initial", StringType(), True),
    StructField("source_data_carga_raw_final", StringType(), True),
    StructField("source_data_carga_initial", StringType(), True),
    StructField("source_data_carga_final", StringType(), True),
    StructField("control_zip_sha256", StringType(), True),
    StructField("total_datasets", IntegerType(), True),
    StructField("successful_datasets", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("retention_status", StringType(), True)
])

MANIFEST_SCHEMA = StructType([
    StructField("ingestion_run_id", StringType(), False),
    StructField("dataset_id", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("source_zip_file", StringType(), True),
    StructField("source_member_file", StringType(), True),
    StructField("source_sha256", StringType(), True),
    StructField("file_size_bytes", LongType(), True),
    StructField("retrieved_at_utc", StringType(), True),
    StructField("ingested_at_utc", StringType(), True),
    StructField("encoding", StringType(), True),
    StructField("delimiter", StringType(), True),
    StructField("columns_json", StringType(), True),
    StructField("logical_input_rows", LongType(), True),
    StructField("delta_output_rows", LongType(), True),
    StructField("raw_s3_key", StringType(), True),
    StructField("delta_table_path", StringType(), True),
    StructField("delta_version", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("status", StringType(), False),
    StructField("reuse_reason", StringType(), True),
    StructField("original_run_id", StringType(), True),
    StructField("retention_status", StringType(), True),
    StructField("retention_deleted_at_utc", StringType(), True),
    StructField("error_message", StringType(), True)
])

class AuditManager:
    def __init__(self, spark: SparkSession, warehouse_prefix: str = "s3a://bronze/warehouse"):
        self.spark = spark
        self.runs_path = f"{warehouse_prefix}/ingestion_runs"
        self.manifest_path = f"{warehouse_prefix}/ingestion_manifest"
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Inicializa as tabelas Delta caso ainda não existam."""
        try:
            self.spark.read.format("delta").load(self.runs_path)
        except Exception:
            logger.info(f"Criando tabela Delta inicial de runs em {self.runs_path}...")
            empty_runs = self.spark.createDataFrame([], RUNS_SCHEMA)
            empty_runs.write.format("delta").mode("overwrite").save(self.runs_path)

        try:
            self.spark.read.format("delta").load(self.manifest_path)
        except Exception:
            logger.info(f"Criando tabela Delta inicial de manifest em {self.manifest_path}...")
            empty_manifest = self.spark.createDataFrame([], MANIFEST_SCHEMA)
            empty_manifest.write.format("delta").mode("overwrite").save(self.manifest_path)

    def start_run(
        self,
        run_id: str,
        force: bool,
        control_info: Optional[Dict[str, Any]] = None,
        total_datasets: int = 4
    ):
        """Registra o início de uma execução com status RUNNING."""
        now_utc = datetime.now(timezone.utc).isoformat()
        row_data = [(
            run_id,
            now_utc,
            None,
            "RUNNING",
            force,
            control_info.get("source_data_carga_raw") if control_info else None,
            None,
            control_info.get("source_data_carga") if control_info else None,
            None,
            control_info.get("sha256") if control_info else None,
            total_datasets,
            0,
            None,
            "PENDING"
        )]
        self._upsert_run(row_data)

    def finish_run(
        self,
        run_id: str,
        status: str,
        control_final: Optional[Dict[str, Any]] = None,
        successful_datasets: int = 0,
        error_message: Optional[str] = None,
        retention_status: str = "SKIPPED"
    ):
        """Atualiza a execução para o status final (SUCCESS, FAILED ou NO_CHANGE)."""
        now_utc = datetime.now(timezone.utc).isoformat()

        # Carregar linha existente para preservar campos iniciais
        existing_df = self.spark.read.format("delta").load(self.runs_path).filter(F.col("ingestion_run_id") == run_id)
        if existing_df.count() > 0:
            row = existing_df.collect()[0]
            start_time = row["start_time_utc"]
            force = row["force"]
            raw_initial = row["source_data_carga_raw_initial"]
            dt_initial = row["source_data_carga_initial"]
            ctrl_sha = row["control_zip_sha256"]
            total_ds = row["total_datasets"]
        else:
            start_time = now_utc
            force = False
            raw_initial = control_final.get("source_data_carga_raw") if control_final else None
            dt_initial = control_final.get("source_data_carga") if control_final else None
            ctrl_sha = control_final.get("sha256") if control_final else None
            total_ds = 4

        row_data = [(
            run_id,
            start_time,
            now_utc,
            status,
            force,
            raw_initial,
            control_final.get("source_data_carga_raw") if control_final else raw_initial,
            dt_initial,
            control_final.get("source_data_carga") if control_final else dt_initial,
            ctrl_sha,
            total_ds,
            successful_datasets,
            error_message,
            retention_status
        )]
        self._upsert_run(row_data)

    def _upsert_run(self, row_data: List[tuple]):
        """Atualiza de forma idempotente a linha da execução na tabela Delta."""
        new_df = self.spark.createDataFrame(row_data, RUNS_SCHEMA)
        existing_df = self.spark.read.format("delta").load(self.runs_path)

        # Filtra o run_id atual e realiza união para overwrite idempotente
        cleaned_df = existing_df.filter(F.col("ingestion_run_id") != row_data[0][0])
        combined_df = cleaned_df.unionByName(new_df)
        combined_df.write.format("delta").mode("overwrite").save(self.runs_path)

    def record_manifest(self, manifest_entry: Dict[str, Any]):
        """Grava de forma idempotente uma entrada no manifesto por (run_id, dataset_id)."""
        row_tuple = (
            manifest_entry["ingestion_run_id"],
            manifest_entry["dataset_id"],
            manifest_entry.get("source_url"),
            manifest_entry.get("source_zip_file"),
            manifest_entry.get("source_member_file"),
            manifest_entry.get("source_sha256"),
            manifest_entry.get("file_size_bytes"),
            manifest_entry.get("retrieved_at_utc"),
            manifest_entry.get("ingested_at_utc"),
            manifest_entry.get("encoding"),
            manifest_entry.get("delimiter"),
            json.dumps(manifest_entry.get("columns", [])) if isinstance(manifest_entry.get("columns"), list) else manifest_entry.get("columns_json"),
            manifest_entry.get("logical_input_rows"),
            manifest_entry.get("delta_output_rows"),
            manifest_entry.get("raw_s3_key"),
            manifest_entry.get("delta_table_path"),
            manifest_entry.get("delta_version"),
            manifest_entry.get("duration_seconds"),
            manifest_entry.get("status", "SUCCESS"),
            manifest_entry.get("reuse_reason"),
            manifest_entry.get("original_run_id"),
            manifest_entry.get("retention_status", "ACTIVE"),
            manifest_entry.get("retention_deleted_at_utc"),
            manifest_entry.get("error_message")
        )

        new_df = self.spark.createDataFrame([row_tuple], MANIFEST_SCHEMA)
        existing_df = self.spark.read.format("delta").load(self.manifest_path)

        run_id = manifest_entry["ingestion_run_id"]
        dataset_id = manifest_entry["dataset_id"]

        cleaned_df = existing_df.filter(
            ~((F.col("ingestion_run_id") == run_id) & (F.col("dataset_id") == dataset_id))
        )
        combined_df = cleaned_df.unionByName(new_df)
        combined_df.write.format("delta").mode("overwrite").save(self.manifest_path)

    def get_last_successful_run(self) -> Optional[Dict[str, Any]]:
        """Retorna os dados da última execução com status SUCCESS ordenada por end_time_utc."""
        df = self.spark.read.format("delta").load(self.runs_path)
        success_df = df.filter(F.col("status") == "SUCCESS").orderBy(F.col("end_time_utc").desc())

        if success_df.count() == 0:
            return None

        row = success_df.collect()[0]
        return row.asDict()

    def get_active_manifest_for_dataset(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Retorna o manifesto ativo mais recente para determinado dataset."""
        df = self.spark.read.format("delta").load(self.manifest_path)
        active_df = df.filter(
            (F.col("dataset_id") == dataset_id) & (F.col("status") == "SUCCESS")
        ).orderBy(F.col("ingested_at_utc").desc())

        if active_df.count() == 0:
            return None
        return active_df.collect()[0].asDict()

    def update_retention_records(self, deleted_keys: List[str]):
        """Atualiza no manifesto os objetos cuja exclusão por retenção foi confirmada."""
        if not deleted_keys:
            return

        now_utc = datetime.now(timezone.utc).isoformat()
        df = self.spark.read.format("delta").load(self.manifest_path)

        updated_df = df.withColumn(
            "retention_status",
            F.when(F.col("raw_s3_key").isin(deleted_keys), "DELETED").otherwise(F.col("retention_status"))
        ).withColumn(
            "retention_deleted_at_utc",
            F.when(F.col("raw_s3_key").isin(deleted_keys), now_utc).otherwise(F.col("retention_deleted_at_utc"))
        )
        updated_df.write.format("delta").mode("overwrite").save(self.manifest_path)
