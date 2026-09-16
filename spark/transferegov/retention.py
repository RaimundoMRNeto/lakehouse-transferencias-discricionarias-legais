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

def calculate_retention_plan(
    spark: SparkSession,
    s3_mgr: S3StorageManager,
    config: AppConfig
) -> Dict[str, Any]:
    """
    Calcula o plano de retenção por dataset:
    - Identifica as duas últimas revisões distintas ativadas com sucesso via manifesto.
    - Mapeia os objetos físicos existentes no MinIO/S3.
    - Classifica cada chave como PROTEGIDA ou CANDIDATA À EXCLUSÃO.
    """
    manifest_path = f"{config.storage.warehouse_prefix}/ingestion_manifest"
    runs_path = f"{config.storage.warehouse_prefix}/ingestion_runs"

    # Identificar execuções de sucesso
    runs_df = spark.read.format("delta").load(f"s3a://bronze/{runs_path}").filter(F.col("status") == "SUCCESS")
    successful_run_ids = [r["ingestion_run_id"] for r in runs_df.select("ingestion_run_id").collect()]

    manifest_df = spark.read.format("delta").load(f"s3a://bronze/{manifest_path}").filter(
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
            .select("source_sha256", "raw_s3_key")
            .collect()
        )

        # Encontrar até 2 hashes distintos mais recentes ativados com sucesso
        distinct_successful_hashes: List[str] = []
        protected_ds_keys: Set[str] = set()

        for row in ds_manifests:
            sha = row["source_sha256"]
            if sha and sha not in distinct_successful_hashes:
                distinct_successful_hashes.append(sha)
            if sha in distinct_successful_hashes[:config.raw_versions_per_dataset]:
                if row["raw_s3_key"]:
                    protected_ds_keys.add(row["raw_s3_key"])

        # Listar objetos existentes no storage físico
        physical_objects = s3_mgr.list_dataset_raw_objects(ds_id)
        ds_candidates: List[str] = []

        for obj in physical_objects:
            key = obj["key"]
            # Extrair sha256 da chave: raw/transferegov/<dataset>/sha256=<hash>/<arquivo>
            match = re.search(r"sha256=([a-fA-F0-9]+)", key)
            obj_sha = match.group(1) if match else None

            # Protegido se pertencer a um dos hashes protegidos
            if obj_sha and obj_sha in distinct_successful_hashes[:config.raw_versions_per_dataset]:
                protected_keys.append(key)
            elif key in protected_ds_keys:
                protected_keys.append(key)
            else:
                # Fora das versões protegidas -> candidato à remoção
                deletion_candidates.append(key)
                ds_candidates.append(key)

        details[ds_id] = {
            "distinct_successful_hashes": distinct_successful_hashes,
            "protected_hashes": distinct_successful_hashes[:config.raw_versions_per_dataset],
            "protected_keys_count": len([k for k in protected_keys if f"/{ds_id}/" in k]),
            "deletion_candidates": ds_candidates
        }

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
