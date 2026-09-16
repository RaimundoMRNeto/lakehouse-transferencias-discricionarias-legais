"""
Script principal de orquestração do pipeline R2 — Ingestão/Bronze Transferegov.
Suporta execução granular por tarefas (Airflow) ou execução completa ponta a ponta.
"""
import os
import sys
import uuid
import time
import json
import shutil
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from pyspark.sql import functions as F, SparkSession

# Adiciona o diretório atual ao sys.path para garantir importação do pacote transferegov
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from transferegov.config import AppConfig
from transferegov.http_downloader import download_file
from transferegov.s3_storage import S3StorageManager
from transferegov.control import fetch_and_validate_control, check_if_no_change_eligible
from transferegov.csv_processor import extract_expected_member, validate_and_count_csv
from transferegov.bronze_writer import get_spark_session, write_bronze_delta_table
from transferegov.catalog import CatalogManager
from transferegov.audit import AuditManager
from transferegov.retention import calculate_retention_plan, execute_retention

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingest_transferegov")

def bootstrap_audit_and_catalog(
    config: AppConfig,
    spark: Optional[SparkSession] = None,
    audit_mgr: Optional[AuditManager] = None,
    catalog_mgr: Optional[CatalogManager] = None
) -> Tuple[AuditManager, CatalogManager]:
    """
    Executa o bootstrap limpo e seguro das tabelas de auditoria (Finding 1):
    1. Obter SparkSession
    2. Instanciar AuditManager
    3. AuditManager verifica/cria com segurança física Delta:
       - s3a://bronze/warehouse/ingestion_runs
       - s3a://bronze/warehouse/ingestion_manifest
    4. Somente depois:
       - Assegurar schema bronze no Thrift Server
       - Registrar ingestion_runs no Thrift
       - Registrar ingestion_manifest no Thrift
    5. Validar leitura das tabelas pelo Thrift via query_count.
    """
    warehouse_location = f"s3a://{config.storage.s3_bucket_bronze}/{config.storage.warehouse_prefix}"
    if spark is None and audit_mgr is None:
        spark = get_spark_session(config)

    if audit_mgr is None:
        audit_mgr = AuditManager(spark, warehouse_prefix=warehouse_location)

    if catalog_mgr is None:
        catalog_mgr = CatalogManager(config.spark.thrift_host, config.spark.thrift_port)

    # 4. Assegurar schema bronze no Thrift e registrar tabelas Delta físicas já válidas
    catalog_mgr.ensure_schema("bronze", warehouse_location)
    catalog_mgr.register_delta_table("bronze", "ingestion_runs", audit_mgr.runs_path)
    catalog_mgr.register_delta_table("bronze", "ingestion_manifest", audit_mgr.manifest_path)

    # 5. Validar leitura das tabelas pelo Thrift
    catalog_mgr.query_count("bronze", "ingestion_runs")
    catalog_mgr.query_count("bronze", "ingestion_manifest")

    return audit_mgr, catalog_mgr

def run_preflight(config: AppConfig, run_id: str) -> bool:
    """Verifica conectividade com serviços, espaço em disco e diretórios temporários."""
    logger.info(f"[{run_id}] Executando verificação de preflight...")

    # Criar diretórios locais
    landing_dir = os.path.join(config.storage.local_landing_dir, run_id)
    staging_dir = os.path.join(config.storage.local_staging_dir, run_id)
    os.makedirs(landing_dir, exist_ok=True)
    os.makedirs(staging_dir, exist_ok=True)

    # Validar conexão S3 / MinIO
    s3_mgr = S3StorageManager(config.storage)
    s3_mgr.s3_client.list_buckets()

    # Bootstrap seguro e ordenado das tabelas de auditoria física e catálogo Thrift
    bootstrap_audit_and_catalog(config)

    logger.info(f"[{run_id}] Preflight concluído com sucesso.")
    return True

def run_control_initial(config: AppConfig, run_id: str, force: bool) -> Dict[str, Any]:
    """Baixa o controle oficial data_carga_siconv e decide se há necessidade de carga."""
    logger.info(f"[{run_id}] Executando controle inicial (force={force})...")
    s3_mgr = S3StorageManager(config.storage)
    spark = get_spark_session(config)
    audit_mgr = AuditManager(spark)
    catalog_mgr = CatalogManager(config.spark.thrift_host, config.spark.thrift_port)

    # Obter e parsear o controle oficial
    control_info = fetch_and_validate_control(config, run_id, s3_mgr)

    # Registrar início da execução na auditoria
    audit_mgr.start_run(run_id, force=force, control_info=control_info, total_datasets=len(config.datasets))

    # Obter última execução com sucesso
    last_success = audit_mgr.get_last_successful_run()

    # Função para verificar integridade do estado local atual
    def verify_local_integrity():
        registered_tables = catalog_mgr.list_tables("bronze")
        for ds_id, ds_cfg in config.datasets.items():
            if ds_cfg.table_name not in registered_tables:
                return False, f"Tabela {ds_cfg.table_name} não encontrada no catálogo Thrift"
            try:
                cnt = catalog_mgr.query_count("bronze", ds_cfg.table_name)
                if cnt == 0:
                    return False, f"Tabela {ds_cfg.table_name} no Thrift possui 0 registros"
            except Exception as e:
                return False, f"Falha na consulta SQL da tabela {ds_cfg.table_name}: {e}"

            # Verificar se há manifesto ativo pertencente a execução com sucesso global
            active_m = audit_mgr.get_active_manifest_for_dataset(ds_id)
            if not active_m:
                return False, f"Nenhum manifesto ativo de execução SUCCESS para dataset {ds_id}"

            # Verificar se o arquivo RAW correspondente existe no MinIO
            raw_key = active_m.get("raw_s3_key")
            if not raw_key or not s3_mgr.raw_key_exists(raw_key):
                return False, f"Arquivo RAW {raw_key} ausente no MinIO para dataset {ds_id}"

        return True, "Todas as 4 tabelas e arquivos RAW estão íntegros e legíveis"

    is_no_change, reason = check_if_no_change_eligible(
        last_successful_run=last_success,
        current_control=control_info,
        integrity_verifier_func=verify_local_integrity,
        force=force
    )

    logger.info(f"[{run_id}] Avaliação de necessidade de carga: is_no_change={is_no_change} ({reason})")

    # Persistir explicitamente a evidência de controle inicial e decisão na árvore do run
    run_staging = os.path.join(config.storage.local_staging_dir, run_id)
    os.makedirs(run_staging, exist_ok=True)
    initial_file = os.path.join(run_staging, "control_initial.json")
    with open(initial_file, "w", encoding="utf-8") as f:
        json.dump(control_info, f, ensure_ascii=False, indent=2)

    decision_file = os.path.join(run_staging, "decision.txt")
    with open(decision_file, "w", encoding="utf-8") as f:
        f.write("STATUS_NO_CHANGE" if is_no_change else "STATUS_CONTINUE")

    if is_no_change:
        # Registrar finalização NO_CHANGE
        audit_mgr.finish_run(
            run_id=run_id,
            status="NO_CHANGE",
            control_final=control_info,
            successful_datasets=len(config.datasets),
            retention_status="SKIPPED"
        )

    return {
        "run_id": run_id,
        "is_no_change": is_no_change,
        "reason": reason,
        "control_info": control_info
    }

def run_ingest_dataset(config: AppConfig, run_id: str, dataset_id: str, force: bool) -> Dict[str, Any]:
    """Processa a ingestão de um dataset analítico específico de forma idempotente."""
    logger.info(f"[{run_id}] Iniciando processamento do dataset '{dataset_id}' (force={force})...")
    start_time = time.time()
    now_utc = datetime.now(timezone.utc).isoformat()

    if dataset_id not in config.datasets:
        raise ValueError(f"Dataset '{dataset_id}' desconhecido na configuração.")

    ds_cfg = config.datasets[dataset_id]
    s3_mgr = S3StorageManager(config.storage)
    spark = get_spark_session(config)
    audit_mgr = AuditManager(spark)
    catalog_mgr = CatalogManager(config.spark.thrift_host, config.spark.thrift_port)

    landing_dir = os.path.join(config.storage.local_landing_dir, run_id, dataset_id)
    staging_dir = os.path.join(config.storage.local_staging_dir, run_id, dataset_id)
    zip_dest_path = os.path.join(landing_dir, ds_cfg.zip_file)
    dataset_url = config.base_url + ds_cfg.zip_file

    try:
        # 1. Download por blocos com cálculo de SHA-256 em streaming
        dl_meta = download_file(
            url=dataset_url,
            destination_path=zip_dest_path,
            chunk_size=65536
        )
        calculated_sha = dl_meta["sha256"]

        # 2. Upload RAW original para MinIO
        upload_meta = s3_mgr.upload_raw_zip(
            local_file_path=zip_dest_path,
            dataset_id=dataset_id,
            calculated_sha256=calculated_sha
        )

        # 3. Comparação de SHA com o snapshot ATUAL válido
        active_manifest = audit_mgr.get_active_manifest_for_dataset(dataset_id)
        current_sha = active_manifest.get("source_sha256") if active_manifest else None

        # Verificar se a tabela Delta existe e é legível
        table_is_valid = False
        if current_sha and active_manifest:
            try:
                cnt = catalog_mgr.query_count("bronze", ds_cfg.table_name)
                if cnt > 0:
                    table_is_valid = True
            except Exception:
                table_is_valid = False

        # Se mesmo SHA atual e tabela válida e não for force -> reaproveitar sem reescrever
        if current_sha == calculated_sha and table_is_valid and not force:
            logger.info(
                f"[{run_id}] Dataset '{dataset_id}' possui mesmo SHA-256 ({calculated_sha}) "
                f"do snapshot atual válido e tabela íntegra. Reaproveitando snapshot existente."
            )
            duration = time.time() - start_time
            manifest_entry = {
                "ingestion_run_id": run_id,
                "dataset_id": dataset_id,
                "source_url": dataset_url,
                "source_zip_file": ds_cfg.zip_file,
                "source_member_file": ds_cfg.member_file,
                "source_sha256": calculated_sha,
                "file_size_bytes": dl_meta["file_size_bytes"],
                "retrieved_at_utc": dl_meta["retrieved_at_utc"],
                "ingested_at_utc": active_manifest["ingested_at_utc"],
                "encoding": active_manifest["encoding"],
                "delimiter": active_manifest["delimiter"],
                "columns_json": active_manifest["columns_json"],
                "logical_input_rows": active_manifest["logical_input_rows"],
                "delta_output_rows": active_manifest["delta_output_rows"],
                "raw_s3_key": upload_meta["s3_key"],
                "delta_table_path": ds_cfg.delta_path,
                "delta_version": active_manifest["delta_version"],
                "duration_seconds": duration,
                "status": "SUCCESS",
                "reuse_reason": f"Reaproveitado do run {active_manifest['ingestion_run_id']} (SHA idêntico ao snapshot atual)",
                "original_run_id": active_manifest.get("original_run_id") or active_manifest["ingestion_run_id"],
                "retention_status": "ACTIVE",
                "error_message": None
            }
            audit_mgr.record_manifest(manifest_entry)
            return manifest_entry

        # 4. Caso contrário (SHA novo, estado ausente ou force=True): Ingerir novo snapshot
        logger.info(f"[{run_id}] Processando nova ingestão para dataset '{dataset_id}'...")

        # Extração restrita do membro CSV
        csv_path = extract_expected_member(
            zip_path=zip_dest_path,
            expected_member_name=ds_cfg.member_file,
            target_dir=staging_dir
        )

        # Validação estrutural do CSV e contagem lógica independente
        csv_validation = validate_and_count_csv(
            csv_path=csv_path,
            delimiter=config.csv_options.get("delimiter", ";"),
            encoding=config.csv_options.get("encoding", "utf-8-sig")
        )

        # Gravação Delta no Spark com StringType em todos os campos oficiais
        delta_result = write_bronze_delta_table(
            spark=spark,
            csv_path=csv_path,
            header_columns=csv_validation["header_columns"],
            dataset_cfg=ds_cfg,
            run_id=run_id,
            source_file=ds_cfg.zip_file,
            source_sha256=calculated_sha,
            logical_row_count=csv_validation["logical_row_count"]
        )

        # Registro / Refresh no Catálogo Spark Thrift Server
        catalog_mgr.register_delta_table("bronze", ds_cfg.table_name, ds_cfg.delta_path)

        # Validação externa de leitura via Thrift Server
        thrift_count = catalog_mgr.query_count("bronze", ds_cfg.table_name)
        if thrift_count != delta_result["delta_output_rows"]:
            raise RuntimeError(
                f"Inconsistência de consulta via Thrift Server para {ds_cfg.table_name}: "
                f"Thrift retornou {thrift_count}, mas Delta gravou {delta_result['delta_output_rows']}."
            )

        duration = time.time() - start_time
        manifest_entry = {
            "ingestion_run_id": run_id,
            "dataset_id": dataset_id,
            "source_url": dataset_url,
            "source_zip_file": ds_cfg.zip_file,
            "source_member_file": ds_cfg.member_file,
            "source_sha256": calculated_sha,
            "file_size_bytes": dl_meta["file_size_bytes"],
            "retrieved_at_utc": dl_meta["retrieved_at_utc"],
            "ingested_at_utc": delta_result["ingested_at_utc"],
            "encoding": csv_validation["encoding"],
            "delimiter": csv_validation["delimiter"],
            "columns": csv_validation["header_columns"],
            "logical_input_rows": csv_validation["logical_row_count"],
            "delta_output_rows": delta_result["delta_output_rows"],
            "raw_s3_key": upload_meta["s3_key"],
            "delta_table_path": ds_cfg.delta_path,
            "delta_version": delta_result["delta_version"],
            "duration_seconds": duration,
            "status": "SUCCESS",
            "reuse_reason": None,
            "original_run_id": run_id,
            "retention_status": "ACTIVE",
            "error_message": None
        }
        audit_mgr.record_manifest(manifest_entry)
        logger.info(f"[{run_id}] Dataset '{dataset_id}' concluído com sucesso em {duration:.2f}s.")
        return manifest_entry

    except Exception as exc:
        duration = time.time() - start_time
        logger.error(f"[{run_id}] Falha na ingestão do dataset '{dataset_id}': {exc}", exc_info=True)
        manifest_entry = {
            "ingestion_run_id": run_id,
            "dataset_id": dataset_id,
            "source_url": dataset_url,
            "source_zip_file": ds_cfg.zip_file,
            "source_member_file": ds_cfg.member_file,
            "source_sha256": None,
            "file_size_bytes": 0,
            "retrieved_at_utc": now_utc,
            "ingested_at_utc": None,
            "encoding": None,
            "delimiter": None,
            "columns": [],
            "logical_input_rows": 0,
            "delta_output_rows": 0,
            "raw_s3_key": None,
            "delta_table_path": ds_cfg.delta_path,
            "delta_version": None,
            "duration_seconds": duration,
            "status": "FAILED",
            "reuse_reason": None,
            "original_run_id": None,
            "retention_status": "FAILED",
            "error_message": str(exc)
        }
        try:
            audit_mgr.record_manifest(manifest_entry)
        except Exception:
            pass
        raise

def run_control_final(config: AppConfig, run_id: str) -> Dict[str, Any]:
    """Relê o controle oficial data_carga_siconv para verificar consistência temporal."""
    logger.info(f"[{run_id}] Executando controle final de consistência...")
    s3_mgr = S3StorageManager(config.storage)
    control_final = fetch_and_validate_control(config, f"{run_id}_final", s3_mgr)

    # Persistir explicitamente a evidência de controle final na árvore temporária do run
    run_staging = os.path.join(config.storage.local_staging_dir, run_id)
    os.makedirs(run_staging, exist_ok=True)
    final_file = os.path.join(run_staging, "control_final.json")
    with open(final_file, "w", encoding="utf-8") as f:
        json.dump(control_final, f, ensure_ascii=False, indent=2)

    return control_final

def run_validate_global(
    config: AppConfig,
    run_id: str,
    control_initial_info: Optional[Dict[str, Any]] = None,
    control_final_info: Optional[Dict[str, Any]] = None,
    audit_mgr: Optional[AuditManager] = None
) -> bool:
    """
    Valida a integridade global da carga:
    - Consome as evidências de controle inicial e final coletadas antes e depois dos datasets.
    - Confirma se todos os 4 datasets analíticos obtiveram status SUCCESS no manifesto.
    - Compara data_carga inicial e final (source_data_carga_raw) para detectar inconsistências.
    - Registra formalmente a revisão ativada do controle data_carga_siconv no manifesto.
    - Atualiza bronze.ingestion_runs para SUCCESS.
    """
    logger.info(f"[{run_id}] Executando validação global da carga...")
    if audit_mgr is None:
        spark = get_spark_session(config)
        audit_mgr = AuditManager(spark)
    else:
        spark = getattr(audit_mgr, "spark", None)

    run_staging = os.path.join(config.storage.local_staging_dir, run_id)
    initial_file = os.path.join(run_staging, "control_initial.json")
    final_file = os.path.join(run_staging, "control_final.json")

    # Se não foram fornecidos diretamente em memória, carregar dos arquivos persistidos pelo run
    if control_initial_info is None:
        if not os.path.exists(initial_file):
            raise RuntimeError(f"Evidência de controle inicial ausente para a execução {run_id} ({initial_file}).")
        with open(initial_file, "r", encoding="utf-8") as f:
            control_initial_info = json.load(f)

    if control_final_info is None:
        if not os.path.exists(final_file):
            raise RuntimeError(f"Evidência de controle final ausente para a execução {run_id} ({final_file}).")
        with open(final_file, "r", encoding="utf-8") as f:
            control_final_info = json.load(f)

    # Verificar consistência de data_carga
    raw_init = control_initial_info.get("source_data_carga_raw")
    raw_final = control_final_info.get("source_data_carga_raw")

    if raw_init != raw_final:
        err = f"Inconsistência temporal detectada: data_carga mudou durante a carga de '{raw_init}' para '{raw_final}'."
        logger.error(err)
        audit_mgr.finish_run(
            run_id=run_id,
            status="FAILED",
            control_final=control_final_info,
            successful_datasets=0,
            error_message=err,
            retention_status="SKIPPED"
        )
        raise RuntimeError(err)

    # Verificar manifestos para este run_id (apenas os 4 datasets analíticos)
    manifest_df = spark.read.format("delta").load(audit_mgr.manifest_path).filter(
        f"ingestion_run_id = '{run_id}' AND status = 'SUCCESS' AND dataset_id != '{config.control.id}'"
    )
    success_manifests = manifest_df.collect()
    successful_count = len(success_manifests)

    if successful_count < len(config.datasets):
        err = f"Publicação parcial detectada: apenas {successful_count} de {len(config.datasets)} datasets com sucesso."
        logger.error(err)
        audit_mgr.finish_run(
            run_id=run_id,
            status="FAILED",
            control_final=control_final_info,
            successful_datasets=successful_count,
            error_message=err,
            retention_status="SKIPPED"
        )
        raise RuntimeError(err)

    # Ativar data_carga_siconv no manifesto somente após sucesso comprovado
    audit_mgr.record_control_manifest(run_id, control_final_info)

    # Atualizar execução para SUCCESS mantendo successful_datasets contando os datasets analíticos
    audit_mgr.finish_run(
        run_id=run_id,
        status="SUCCESS",
        control_final=control_final_info,
        successful_datasets=successful_count,
        retention_status="PENDING"
    )

    logger.info(f"[{run_id}] Validação global concluída com sucesso para os {successful_count} datasets analíticos e controle ativado.")
    return True

def run_retention_step(config: AppConfig, run_id: str, dry_run: bool = False) -> Dict[str, Any]:
    """Executa o cálculo e aplicação da retenção de versões RAW (atual + anterior)."""
    logger.info(f"[{run_id}] Executando etapa de retenção RAW (dry_run={dry_run})...")
    s3_mgr = S3StorageManager(config.storage)
    spark = get_spark_session(config)
    audit_mgr = AuditManager(spark)

    # Verificar se a execução atual foi globalmente validada com sucesso
    runs_df = spark.read.format("delta").load(audit_mgr.runs_path).filter(F.col("ingestion_run_id") == run_id)
    if runs_df.count() == 0 or runs_df.collect()[0]["status"] != "SUCCESS":
        logger.warning(f"[{run_id}] Retenção ignorada: execução não possui status SUCCESS confirmado.")
        return {"status": "SKIPPED", "reason": "Execução não validada como SUCCESS"}

    plan = calculate_retention_plan(spark, s3_mgr, config)
    result = execute_retention(plan, s3_mgr, audit_mgr, dry_run=dry_run)

    # Atualizar status da retenção na tabela de runs
    runs_df_all = spark.read.format("delta").load(audit_mgr.runs_path)
    updated_runs = runs_df_all.withColumn(
        "retention_status",
        F.when(F.col("ingestion_run_id") == run_id, result["status"]).otherwise(F.col("retention_status"))
    )
    updated_runs.write.format("delta").mode("overwrite").save(audit_mgr.runs_path)

    logger.info(f"[{run_id}] Retenção concluída com resultado: {result}")
    return result

def run_cleanup_step(config: AppConfig, run_id: str):
    """Remove arquivos temporários estritamente pertencentes à execução run_id."""
    logger.info(f"[{run_id}] Limpando diretórios temporários da execução...")
    landing_dir = os.path.join(config.storage.local_landing_dir, run_id)
    staging_dir = os.path.join(config.storage.local_staging_dir, run_id)

    for p in [landing_dir, staging_dir, f"{landing_dir}_final", f"{staging_dir}_final"]:
        if os.path.exists(p):
            try:
                shutil.rmtree(p)
                logger.info(f"Diretório temporário removido: {p}")
            except Exception as e:
                logger.warning(f"Aviso ao remover {p}: {e}")

def run_full_pipeline(config: AppConfig, run_id: Optional[str] = None, force: bool = False, dry_run_retention: bool = False):
    """Executa a sequência completa de ponta a ponta."""
    if not run_id:
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    logger.info(f"==================================================")
    logger.info(f"INICIANDO PIPELINE R2 BRONZE [run_id={run_id}, force={force}]")
    logger.info(f"==================================================")

    run_preflight(config, run_id)

    ctrl_initial = run_control_initial(config, run_id, force=force)
    if ctrl_initial["is_no_change"]:
        logger.info(f"[{run_id}] Pipeline encerrado com status NO_CHANGE.")
        run_cleanup_step(config, run_id)
        return {"run_id": run_id, "status": "NO_CHANGE"}

    # Ingestão sequencial dos 4 datasets
    for ds_id in config.datasets.keys():
        run_ingest_dataset(config, run_id, ds_id, force=force)

    # Controle final
    ctrl_final = run_control_final(config, run_id)

    # Validação global
    run_validate_global(config, run_id, ctrl_initial["control_info"], ctrl_final)

    # Retenção RAW
    run_retention_step(config, run_id, dry_run=dry_run_retention)

    # Limpeza de temporários
    run_cleanup_step(config, run_id)

    logger.info(f"==================================================")
    logger.info(f"PIPELINE R2 BRONZE CONCLUÍDO COM SUCESSO [run_id={run_id}]")
    logger.info(f"==================================================")
    return {"run_id": run_id, "status": "SUCCESS"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador R2 Ingestão Bronze Transferegov")
    parser.add_argument("--action", choices=[
        "preflight", "control_initial", "ingest_dataset", "control_final",
        "validate_global", "retention", "cleanup", "full_run"
    ], default="full_run", help="Ação a executar")
    parser.add_argument("--run-id", default=None, help="Identificador único da execução")
    parser.add_argument("--dataset", default=None, help="ID do dataset analítico (obrigatório para ingest_dataset)")
    parser.add_argument("--force", action="store_true", help="Forçar nova verificação e ingestão mesmo com data_carga igual")
    parser.add_argument("--dry-run-retention", action="store_true", help="Executar retenção em modo dry-run")
    parser.add_argument("--config", default=None, help="Caminho alternativo para transferegov_sources.yml")

    args = parser.parse_args()
    cfg = AppConfig(args.config)
    cur_run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    if args.action == "preflight":
        run_preflight(cfg, cur_run_id)
    elif args.action == "control_initial":
        ctrl_res = run_control_initial(cfg, cur_run_id, force=args.force)
        status_str = "STATUS_NO_CHANGE" if ctrl_res["is_no_change"] else "STATUS_CONTINUE"
        print(status_str)
    elif args.action == "ingest_dataset":
        if not args.dataset:
            parser.error("--dataset é obrigatório para action=ingest_dataset")
        run_ingest_dataset(cfg, cur_run_id, args.dataset, force=args.force)
    elif args.action == "control_final":
        run_control_final(cfg, cur_run_id)
    elif args.action == "validate_global":
        run_validate_global(cfg, cur_run_id)
    elif args.action == "retention":
        run_retention_step(cfg, cur_run_id, dry_run=args.dry_run_retention)
    elif args.action == "cleanup":
        run_cleanup_step(cfg, cur_run_id)
    elif args.action == "full_run":
        run_full_pipeline(cfg, run_id=cur_run_id, force=args.force, dry_run_retention=args.dry_run_retention)
