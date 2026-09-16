"""
Módulo de política de retenção da camada RAW.
Mantém estritamente:
1. A revisão atualmente ativa
2. A revisão distinta imediatamente anterior ativada com sucesso
"""
import os
import re
import logging
from typing import Dict, Any, List, Set
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transferegov.config import AppConfig
from transferegov.s3_storage import S3StorageManager
from transferegov.audit import AuditManager

logger = logging.getLogger(__name__)

def plan_dataset_retention(
    dataset_id: str,
    successful_manifests: List[Dict[str, Any]],
    physical_keys: List[str],
    raw_versions_to_keep: int = 2
) -> Dict[str, Any]:
    """
    Função pura que calcula o plano de retenção para um dataset:
    - Identifica até `raw_versions_to_keep` hashes distintos mais recentes ativados com sucesso via manifesto.
    - Chaves físicas pertencentes a esses hashes (ou registradas no manifesto desses hashes) são PROTEGIDAS.
    - Chaves físicas de versões mais antigas, pertencentes apenas a execuções FAILED,
      ou sem hash reconhecível são classificadas como CANDIDATAS À EXCLUSÃO.
    """
    distinct_successful_hashes: List[str] = []
    protected_manifest_keys: Set[str] = set()

    for row in successful_manifests:
        sha = row.get("source_sha256")
        key = row.get("raw_s3_key")
        if sha and sha not in distinct_successful_hashes:
            distinct_successful_hashes.append(sha)
        if sha in distinct_successful_hashes[:raw_versions_to_keep]:
            if key:
                protected_manifest_keys.add(key)

    protected_hashes = distinct_successful_hashes[:raw_versions_to_keep]

    protected_keys: List[str] = []
    deletion_candidates: List[str] = []

    for key in physical_keys:
        match = re.search(r"sha256=([a-fA-F0-9]+)", key)
        obj_sha = match.group(1) if match else None

        if obj_sha and obj_sha in protected_hashes:
            protected_keys.append(key)
        elif key in protected_manifest_keys:
            protected_keys.append(key)
        else:
            deletion_candidates.append(key)

    return {
        "dataset_id": dataset_id,
        "distinct_successful_hashes": distinct_successful_hashes,
        "protected_hashes": protected_hashes,
        "protected_keys": protected_keys,
        "deletion_candidates": deletion_candidates
    }

def calculate_retention_plan(
    spark: SparkSession,
    s3_mgr: S3StorageManager,
    config: AppConfig,
    warehouse_prefix: str = None
) -> Dict[str, Any]:
    """
    Calcula o plano de retenção por dataset:
    - Identifica as duas últimas revisões distintas ativadas com sucesso via manifesto.
    - Mapeia os objetos físicos existentes no MinIO/S3.
    - Classifica cada chave como PROTEGIDA ou CANDIDATA À EXCLUSÃO.
    """
    wh_prefix = warehouse_prefix or config.storage.warehouse_prefix
    if not wh_prefix.startswith("s3a://"):
        wh_prefix = f"s3a://bronze/{wh_prefix.lstrip('/')}"

    runs_path = f"{wh_prefix}/ingestion_runs"
    manifest_path = f"{wh_prefix}/ingestion_manifest"

    # Identificar execuções de sucesso
    runs_df = spark.read.format("delta").load(runs_path).filter(F.col("status") == "SUCCESS")
    successful_run_ids = [r["ingestion_run_id"] for r in runs_df.select("ingestion_run_id").collect()]

    manifest_df = spark.read.format("delta").load(manifest_path).filter(
        (F.col("status") == "SUCCESS") & (F.col("ingestion_run_id").isin(successful_run_ids))
    )

    all_dataset_ids = list(config.datasets.keys()) + [config.control.id]

    protected_keys: List[str] = []
    deletion_candidates: List[str] = []
    details: Dict[str, Any] = {}

    for ds_id in all_dataset_ids:
        # Obter histórico de hashes bem-sucedidos em ordem cronológica reversa
        ds_manifests = (
            manifest_df.filter(F.col("dataset_id") == ds_id)
            .orderBy(F.col("ingested_at_utc").desc())
            .select("source_sha256", "raw_s3_key", "ingested_at_utc")
            .collect()
        )
        manifest_dicts = [
            {
                "source_sha256": row["source_sha256"],
                "raw_s3_key": row["raw_s3_key"],
                "ingested_at_utc": row["ingested_at_utc"]
            }
            for row in ds_manifests
        ]

        # Listar objetos existentes no storage físico
        physical_objects = s3_mgr.list_dataset_raw_objects(ds_id)
        physical_keys = [obj["key"] for obj in physical_objects]

        ds_plan = plan_dataset_retention(
            dataset_id=ds_id,
            successful_manifests=manifest_dicts,
            physical_keys=physical_keys,
            raw_versions_to_keep=config.raw_versions_per_dataset
        )

        protected_keys.extend(ds_plan["protected_keys"])
        deletion_candidates.extend(ds_plan["deletion_candidates"])
        details[ds_id] = ds_plan

    return {
        "protected_keys": protected_keys,
        "deletion_candidates": deletion_candidates,
        "details": details
    }

def execute_retention(
    plan: Dict[str, Any],
    s3_mgr: S3StorageManager,
    audit_mgr: AuditManager,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Executa a retenção conforme o plano:
    - Se dry_run=True, apenas reporta sem apagar nenhum byte.
    - Se dry_run=False, exclui exclusivamente as chaves exatas planejadas
      e atualiza os registros correspondentes no manifesto de auditoria.
    """
    candidates = plan.get("deletion_candidates", [])

    if dry_run:
        logger.info(f"[DRY-RUN] Retenção planejada para {len(candidates)} objetos. Nenhum objeto excluído.")
        return {
            "status": "DRY_RUN",
            "total_candidates": len(candidates),
            "deleted_keys": [],
            "planned_deletions": candidates
        }

    if not candidates:
        logger.info("Nenhum objeto antigo para exclusão na política de retenção.")
        return {
            "status": "NOOP",
            "total_candidates": 0,
            "deleted_keys": []
        }

    confirmed_deleted: List[str] = []
    failed_deleted: List[str] = []

    for key in candidates:
        try:
            success = s3_mgr.delete_exact_raw_key(key)
            if success:
                confirmed_deleted.append(key)
            else:
                failed_deleted.append(key)
        except Exception as e:
            logger.error(f"Erro ao excluir chave de retenção {key}: {e}")
            failed_deleted.append(key)

    # Atualizar manifesto para os objetos com exclusão física confirmada
    if confirmed_deleted:
        audit_mgr.update_retention_records(confirmed_deleted)

    logger.info(
        f"Retenção finalizada: {len(confirmed_deleted)} objetos confirmados excluídos, "
        f"{len(failed_deleted)} falhas."
    )

    return {
        "status": "EXECUTED" if not failed_deleted else "PARTIAL_FAILURE",
        "total_candidates": len(candidates),
        "deleted_keys": confirmed_deleted,
        "failed_keys": failed_deleted
    }
