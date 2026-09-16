"""
Testes unitários para bootstrap seguro de auditoria/catálogo e integridade do NO_CHANGE.
Executáveis de forma isolada e leve no GitHub Actions (CI) sem necessidade de cluster.
"""
import unittest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

from transferegov.config import StorageConfig, AppConfig
from transferegov.s3_storage import S3StorageManager
from transferegov.audit import find_active_manifest_record, AuditManager
from transferegov.control import check_if_no_change_eligible
from ingest_transferegov import bootstrap_audit_and_catalog


def make_client_error(code: str, message: str = "Error", status_code: int = 400) -> ClientError:
    return ClientError(
        error_response={
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status_code}
        },
        operation_name="HeadObject"
    )


class TestBootstrapSafety(unittest.TestCase):
    """Cobertura do Finding 1 — Ordem correta e segurança de bootstrap das tabelas de auditoria."""

    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.storage = MagicMock()
        self.mock_config.storage.s3_bucket_bronze = "bronze"
        self.mock_config.storage.warehouse_prefix = "warehouse"
        self.mock_config.spark = MagicMock()
        self.mock_config.spark.thrift_host = "spark-thrift-server"
        self.mock_config.spark.thrift_port = 10000

    def test_case_a_clean_environment_bootstrap_order(self):
        """
        Caso A — Ambiente limpo:
        Comprova que AuditManager é inicializado antes do register_delta_table,
        e que as duas tabelas só são registradas no catálogo após estarem disponíveis fisicamente.
        """
        execution_order = []

        mock_audit = MagicMock(spec=AuditManager)
        mock_audit.runs_path = "s3a://bronze/warehouse/ingestion_runs"
        mock_audit.manifest_path = "s3a://bronze/warehouse/ingestion_manifest"

        mock_catalog = MagicMock()
        mock_catalog.ensure_schema.side_effect = lambda *a, **k: execution_order.append("ensure_schema")
        mock_catalog.register_delta_table.side_effect = lambda *a, **k: execution_order.append(f"register_{a[1]}")
        mock_catalog.query_count.side_effect = lambda *a, **k: execution_order.append(f"query_count_{a[1]}")

        def mock_audit_factory(*args, **kwargs):
            execution_order.append("audit_init")
            return mock_audit

        with patch("ingest_transferegov.AuditManager", side_effect=mock_audit_factory):
            with patch("ingest_transferegov.CatalogManager", return_value=mock_catalog):
                audit_res, cat_res = bootstrap_audit_and_catalog(
                    config=self.mock_config,
                    spark=MagicMock()
                )

        self.assertEqual(
            execution_order,
            [
                "audit_init",
                "ensure_schema",
                "register_ingestion_runs",
                "register_ingestion_manifest",
                "query_count_ingestion_runs",
                "query_count_ingestion_manifest"
            ],
            "A ordem estrita de bootstrap deve inicializar AuditManager antes do registro no catálogo"
        )
        mock_catalog.ensure_schema.assert_called_once_with("bronze", "s3a://bronze/warehouse")
        mock_catalog.register_delta_table.assert_has_calls([
            call("bronze", "ingestion_runs", "s3a://bronze/warehouse/ingestion_runs"),
            call("bronze", "ingestion_manifest", "s3a://bronze/warehouse/ingestion_manifest"),
        ])
        mock_catalog.query_count.assert_has_calls([
            call("bronze", "ingestion_runs"),
            call("bronze", "ingestion_manifest"),
        ])

    def test_case_b_existing_valid_environment_preserves_tables(self):
        """
        Caso B — Ambiente existente válido:
        Comprova que o bootstrap não executa criação destrutiva e registra/refresca metadados normalmente.
        """
        mock_audit = MagicMock(spec=AuditManager)
        mock_audit.runs_path = "s3a://bronze/warehouse/ingestion_runs"
        mock_audit.manifest_path = "s3a://bronze/warehouse/ingestion_manifest"

        mock_catalog = MagicMock()
        mock_catalog.query_count.return_value = 10

        audit_res, cat_res = bootstrap_audit_and_catalog(
            config=self.mock_config,
            spark=MagicMock(),
            audit_mgr=mock_audit,
            catalog_mgr=mock_catalog
        )

        self.assertEqual(audit_res, mock_audit)
        self.assertEqual(cat_res, mock_catalog)
        mock_catalog.register_delta_table.assert_has_calls([
            call("bronze", "ingestion_runs", mock_audit.runs_path),
            call("bronze", "ingestion_manifest", mock_audit.manifest_path),
        ])
        mock_catalog.query_count.assert_has_calls([
            call("bronze", "ingestion_runs"),
            call("bronze", "ingestion_manifest"),
        ])

    def test_case_c_corrupted_storage_propagates_error_and_prevents_catalog_registration(self):
        """
        Caso C — Storage existente inválido/corrompido:
        Comprova que o erro é propagado e o catálogo NÃO é registrado como se estivesse tudo bem.
        """
        mock_catalog = MagicMock()

        def mock_audit_failing(*args, **kwargs):
            raise RuntimeError("Tabela Delta de runs corrompida em s3a://bronze/warehouse/ingestion_runs")

        with patch("ingest_transferegov.AuditManager", side_effect=mock_audit_failing):
            with patch("ingest_transferegov.CatalogManager", return_value=mock_catalog):
                with self.assertRaises(RuntimeError) as ctx:
                    bootstrap_audit_and_catalog(config=self.mock_config, spark=MagicMock())

        self.assertIn("corrompida", str(ctx.exception))
        mock_catalog.register_delta_table.assert_not_called()
        mock_catalog.query_count.assert_not_called()


class TestActiveManifestFinding2(unittest.TestCase):
    """Cobertura do Finding 2 — Identificação estrita de snapshot ativo a partir de runs globalmente SUCCESS."""

    def test_scenario_1_manifest_success_of_global_run_success(self):
        """Cenário 1 — Manifesto SUCCESS de run global SUCCESS -> elegível como snapshot ativo."""
        runs = [
            {"ingestion_run_id": "run-001", "status": "SUCCESS", "end_time_utc": "2026-09-16T12:00:00Z"}
        ]
        manifests = [
            {
                "ingestion_run_id": "run-001",
                "dataset_id": "siconv_convenio",
                "status": "SUCCESS",
                "source_sha256": "sha_convenio_v1",
                "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=sha_convenio_v1/siconv_convenio.zip",
                "ingested_at_utc": "2026-09-16T11:55:00Z"
            }
        ]
        active = find_active_manifest_record("siconv_convenio", runs, manifests)
        self.assertIsNotNone(active)
        self.assertEqual(active["source_sha256"], "sha_convenio_v1")

    def test_scenario_2_manifest_success_of_global_run_failed(self):
        """Cenário 2 — Manifesto SUCCESS de run global FAILED -> NÃO pode ser considerado ativo."""
        runs = [
            {"ingestion_run_id": "run-002", "status": "FAILED", "end_time_utc": "2026-09-16T12:00:00Z"}
        ]
        manifests = [
            {
                "ingestion_run_id": "run-002",
                "dataset_id": "siconv_convenio",
                "status": "SUCCESS",
                "source_sha256": "sha_convenio_v2",
                "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=sha_convenio_v2/siconv_convenio.zip",
                "ingested_at_utc": "2026-09-16T11:55:00Z"
            }
        ]
        active = find_active_manifest_record("siconv_convenio", runs, manifests)
        self.assertIsNone(active, "Manifesto de run FAILED não deve ser considerado snapshot ativo")

    def test_scenario_3_manifest_success_of_global_run_running(self):
        """Cenário 3 — Manifesto SUCCESS de run RUNNING -> NÃO pode ser considerado ativo."""
        runs = [
            {"ingestion_run_id": "run-003", "status": "RUNNING", "end_time_utc": None}
        ]
        manifests = [
            {
                "ingestion_run_id": "run-003",
                "dataset_id": "siconv_convenio",
                "status": "SUCCESS",
                "source_sha256": "sha_convenio_v3",
                "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=sha_convenio_v3/siconv_convenio.zip",
                "ingested_at_utc": "2026-09-16T11:55:00Z"
            }
        ]
        active = find_active_manifest_record("siconv_convenio", runs, manifests)
        self.assertIsNone(active, "Manifesto de run em andamento (RUNNING) não deve ser considerado snapshot ativo")

    def test_scenario_4_two_snapshots_old_success_new_failed(self):
        """
        Cenário 4 — Dois snapshots: run antigo SUCCESS, run novo FAILED.
        O snapshot ativo deve continuar sendo o do run antigo SUCCESS.
        """
        runs = [
            {"ingestion_run_id": "run-old", "status": "SUCCESS", "end_time_utc": "2026-09-16T10:00:00Z"},
            {"ingestion_run_id": "run-new", "status": "FAILED", "end_time_utc": "2026-09-16T11:00:00Z"}
        ]
        manifests = [
            {
                "ingestion_run_id": "run-old",
                "dataset_id": "siconv_convenio",
                "status": "SUCCESS",
                "source_sha256": "sha_old_stable",
                "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=sha_old_stable/siconv_convenio.zip",
                "ingested_at_utc": "2026-09-16T09:55:00Z"
            },
            {
                "ingestion_run_id": "run-new",
                "dataset_id": "siconv_convenio",
                "status": "SUCCESS",
                "source_sha256": "sha_new_broken",
                "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=sha_new_broken/siconv_convenio.zip",
                "ingested_at_utc": "2026-09-16T10:55:00Z"
            }
        ]
        active = find_active_manifest_record("siconv_convenio", runs, manifests)
        self.assertIsNotNone(active)
        self.assertEqual(active["ingestion_run_id"], "run-old")
        self.assertEqual(active["source_sha256"], "sha_old_stable")

    def test_two_snapshots_both_success_returns_latest(self):
        """Dois snapshots com sucesso: retorna o mais recente por data de finalização."""
        runs = [
            {"ingestion_run_id": "run-1", "status": "SUCCESS", "end_time_utc": "2026-09-16T10:00:00Z"},
            {"ingestion_run_id": "run-2", "status": "SUCCESS", "end_time_utc": "2026-09-16T12:00:00Z"}
        ]
        manifests = [
            {
                "ingestion_run_id": "run-1",
                "dataset_id": "siconv_programa",
                "status": "SUCCESS",
                "source_sha256": "sha_prog_v1",
                "ingested_at_utc": "2026-09-16T09:55:00Z"
            },
            {
                "ingestion_run_id": "run-2",
                "dataset_id": "siconv_programa",
                "status": "SUCCESS",
                "source_sha256": "sha_prog_v2",
                "ingested_at_utc": "2026-09-16T11:55:00Z"
            }
        ]
        active = find_active_manifest_record("siconv_programa", runs, manifests)
        self.assertEqual(active["ingestion_run_id"], "run-2")
        self.assertEqual(active["source_sha256"], "sha_prog_v2")


class TestNoChangeAndS3Integrity(unittest.TestCase):
    """Cobertura do Finding 2 / Item 4 — Validação de existência RAW e propagação de erros S3."""

    def setUp(self):
        self.storage_cfg = StorageConfig(
            s3_endpoint="http://minio:9000",
            s3_access_key="minio",
            s3_secret_key="minio123",
            s3_bucket_bronze="bronze",
            s3_region="us-east-1",
            raw_prefix="raw/transferegov",
            warehouse_prefix="warehouse",
            local_landing_dir="/data/landing",
            local_staging_dir="/data/staging"
        )
        self.s3_mgr = S3StorageManager(self.storage_cfg)

    def test_scenario_5_raw_present_integrity_valid(self):
        """Cenário 5 — RAW presente: integridade permanece válida e elegível a NO_CHANGE."""
        self.s3_mgr.s3_client = MagicMock()
        self.s3_mgr.s3_client.head_object.return_value = {"ContentLength": 1024}

        key = "raw/transferegov/siconv_convenio/sha256=abc/siconv_convenio.zip"
        self.assertTrue(self.s3_mgr.object_exists(key))
        self.assertTrue(self.s3_mgr.raw_key_exists(key))

        # Teste de verificação de integridade
        def mock_verifier():
            if not self.s3_mgr.raw_key_exists(key):
                return False, "Arquivo RAW ausente"
            return True, "Tudo íntegro"

        last_success = {
            "source_data_carga_raw_final": "16/09/2026 06:32:11",
            "status": "SUCCESS"
        }
        current_control = {"source_data_carga_raw": "16/09/2026 06:32:11"}

        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=mock_verifier,
            force=False
        )
        self.assertTrue(is_no_change)
        self.assertIn("idêntica à última execução com sucesso e estado local íntegro", reason)

    def test_scenario_6_raw_missing_404_rejects_no_change_without_crash(self):
        """
        Cenário 6 — RAW ausente / 404:
        object_exists retorna False, integridade retorna inválida e NO_CHANGE=False (processamento continua).
        """
        self.s3_mgr.s3_client = MagicMock()
        self.s3_mgr.s3_client.head_object.side_effect = make_client_error("404", "Not Found", status_code=404)

        key = "raw/transferegov/siconv_convenio/sha256=abc/siconv_convenio.zip"
        self.assertFalse(self.s3_mgr.object_exists(key))
        self.assertFalse(self.s3_mgr.raw_key_exists(key))

        # NoSuchKey também deve retornar False
        self.s3_mgr.s3_client.head_object.side_effect = make_client_error("NoSuchKey", "The specified key does not exist", status_code=404)
        self.assertFalse(self.s3_mgr.raw_key_exists(key))

        def mock_verifier():
            if not self.s3_mgr.raw_key_exists(key):
                return False, f"Arquivo RAW {key} ausente no MinIO para dataset siconv_convenio"
            return True, "Tudo íntegro"

        last_success = {
            "source_data_carga_raw_final": "16/09/2026 06:32:11",
            "status": "SUCCESS"
        }
        current_control = {"source_data_carga_raw": "16/09/2026 06:32:11"}

        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=mock_verifier,
            force=False
        )
        self.assertFalse(is_no_change)
        self.assertIn("estado local está incompleto", reason)
        self.assertIn("Arquivo RAW", reason)

    def test_scenario_7_s3_infrastructure_error_propagates_exception(self):
        """
        Cenário 7 — Erro S3 real (500 InternalError ou 403 AccessDenied):
        A exceção deve ser propagada imediatamente, nunca convertida silenciosamente em False.
        """
        self.s3_mgr.s3_client = MagicMock()
        key = "raw/transferegov/siconv_convenio/sha256=abc/siconv_convenio.zip"

        # 500 InternalError
        self.s3_mgr.s3_client.head_object.side_effect = make_client_error("InternalError", "S3 error", status_code=500)
        with self.assertRaises(ClientError):
            self.s3_mgr.raw_key_exists(key)

        # 403 AccessDenied
        self.s3_mgr.s3_client.head_object.side_effect = make_client_error("AccessDenied", "Forbidden", status_code=403)
        with self.assertRaises(ClientError):
            self.s3_mgr.raw_key_exists(key)

        # Falha de rede / Timeout genérico
        self.s3_mgr.s3_client.head_object.side_effect = ConnectionResetError("Connection reset by peer")
        with self.assertRaises(ConnectionResetError):
            self.s3_mgr.raw_key_exists(key)


if __name__ == "__main__":
    unittest.main()
