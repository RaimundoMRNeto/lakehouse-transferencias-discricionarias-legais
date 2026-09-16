"""
Testes de integração para gravação Delta Bronze e tabelas de auditoria.
Executados estritamente em namespace isolado e descartável (Finding 4).
Nunca alteram s3a://bronze/warehouse/.
"""
import os
import uuid
import shutil
import unittest
from transferegov.config import AppConfig, DatasetConfig
from transferegov.csv_processor import validate_and_count_csv
from transferegov.bronze_writer import get_spark_session, write_bronze_delta_table
from transferegov.audit import AuditManager
from transferegov.s3_storage import S3StorageManager

class TestDeltaAndAudit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = AppConfig()
        try:
            cls.s3_mgr = S3StorageManager(cls.config.storage)
            cls.s3_mgr.s3_client.list_buckets()
            cls.spark = get_spark_session(cls.config)
        except Exception as e:
            raise unittest.SkipTest(
                f"Cluster Spark e MinIO indisponíveis neste ambiente (validação restrita ao ambiente local R1/R2): {e}"
            )
        cls.test_uuid = uuid.uuid4().hex[:8]
        cls.test_s3_prefix = f"_tests/r2/{cls.test_uuid}"
        cls.isolated_warehouse = f"s3a://bronze/{cls.test_s3_prefix}/warehouse"
        cls.temp_dir_path = f"/data/staging/_test_{cls.test_uuid}"
        os.makedirs(cls.temp_dir_path, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        # 1. Limpeza rigorosa e restrita do namespace isolado no MinIO
        try:
            resp = cls.s3_mgr.s3_client.list_objects_v2(
                Bucket=cls.s3_mgr.bucket,
                Prefix=cls.test_s3_prefix
            )
            for item in resp.get("Contents", []):
                cls.s3_mgr.s3_client.delete_object(
                    Bucket=cls.s3_mgr.bucket,
                    Key=item["Key"]
                )
        except Exception:
            pass

        # 2. Limpeza do diretório local
        if os.path.exists(cls.temp_dir_path):
            shutil.rmtree(cls.temp_dir_path)

    def test_delta_write_preserves_string_type_and_leading_zeros(self):
        """Valida que campos oficiais são gravados como StringType com zeros à esquerda preservados."""
        csv_path = os.path.join(self.temp_dir_path, "synthetic_prog.csv")
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("ID_PROPOSTA;CODIGO;NOME\r\n000123;0045;Projeto Alpha\r\n000124;0046;Projeto Beta\r\n")

        val = validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")

        ds_cfg = DatasetConfig(
            id=f"test_synth_{self.test_uuid}",
            zip_file="test_synthetic.zip",
            member_file="synthetic_prog.csv",
            table_name=f"test_synthetic_{self.test_uuid}",
            delta_path=f"{self.isolated_warehouse}/test_synthetic",
            raw_s3_prefix=f"s3://bronze/{self.test_s3_prefix}/raw/test_synthetic"
        )

        res = write_bronze_delta_table(
            spark=self.spark,
            csv_path=csv_path,
            header_columns=val["header_columns"],
            dataset_cfg=ds_cfg,
            run_id=f"run_test_zeros_{self.test_uuid}",
            source_file="test_synthetic.zip",
            source_sha256="dummyhash123",
            logical_row_count=val["logical_row_count"]
        )

        self.assertEqual(res["delta_output_rows"], 2)
        self.assertEqual(res["logical_input_rows"], 2)

        # Ler de volta do Delta e verificar zeros à esquerda
        delta_df = self.spark.read.format("delta").load(ds_cfg.delta_path)
        rows = delta_df.collect()
        id_propostas = [r["ID_PROPOSTA"] for r in rows]
        codigos = [r["CODIGO"] for r in rows]

        self.assertIn("000123", id_propostas)
        self.assertIn("000124", id_propostas)
        self.assertIn("0045", codigos)

        # Verificar presença exclusiva dos 4 campos técnicos
        schema_names = delta_df.columns
        for tech_col in ["__ingested_at_utc", "__ingestion_run_id", "__source_file", "__source_sha256"]:
            self.assertIn(tech_col, schema_names)

    def test_audit_idempotency_in_isolated_namespace(self):
        """Valida idempotência da auditoria em namespace isolado de testes."""
        audit_mgr = AuditManager(self.spark, warehouse_prefix=self.isolated_warehouse)
        run_id = f"run_test_audit_idemp_{self.test_uuid}"

        # Registrar início
        audit_mgr.start_run(run_id, force=False, control_info={"source_data_carga_raw": "16/09/2026 06:32:11"})

        # Registrar início novamente com mesmo ID (simulando retry)
        audit_mgr.start_run(run_id, force=False, control_info={"source_data_carga_raw": "16/09/2026 06:32:11"})

        # Deve haver apenas 1 linha para este run_id
        runs_df = self.spark.read.format("delta").load(audit_mgr.runs_path).filter(f"ingestion_run_id = '{run_id}'")
        self.assertEqual(runs_df.count(), 1)

        # Registrar manifesto para o mesmo dataset duas vezes
        manifest_entry = {
            "ingestion_run_id": run_id,
            "dataset_id": "test_ds",
            "source_url": "http://example.com/test.zip",
            "source_zip_file": "test.zip",
            "source_member_file": "test.csv",
            "source_sha256": "sha123",
            "file_size_bytes": 100,
            "retrieved_at_utc": "2026-09-16T12:00:00Z",
            "ingested_at_utc": "2026-09-16T12:05:00Z",
            "encoding": "utf-8-sig",
            "delimiter": ";",
            "columns": ["COL1", "COL2"],
            "logical_input_rows": 10,
            "delta_output_rows": 10,
            "raw_s3_key": f"{self.test_s3_prefix}/raw/test/key",
            "delta_table_path": f"{self.isolated_warehouse}/_test_ds",
            "delta_version": 0,
            "duration_seconds": 1.5,
            "status": "SUCCESS"
        }
        audit_mgr.record_manifest(manifest_entry)
        audit_mgr.record_manifest(manifest_entry)

        # Deve haver apenas 1 linha no manifesto para (run_id, test_ds)
        m_df = self.spark.read.format("delta").load(audit_mgr.manifest_path).filter(
            f"ingestion_run_id = '{run_id}' AND dataset_id = 'test_ds'"
        )
        self.assertEqual(m_df.count(), 1)

    def test_audit_table_initialization_safety(self):
        """
        Testa Finding 3:
        1. Path inexistente -> inicializa tabela vazia.
        2. Path existente válido -> carrega normalmente sem sobrescrita.
        3. Path existente com erro de leitura -> propaga exceção e JAMAIS executa overwrite.
        """
        safety_warehouse = f"{self.isolated_warehouse}/safety_test"

        # Caso 1: Path inexistente -> inicializa com sucesso
        audit_1 = AuditManager(self.spark, warehouse_prefix=safety_warehouse)
        runs_count = self.spark.read.format("delta").load(audit_1.runs_path).count()
        self.assertEqual(runs_count, 0)

        # Inserir um registro para comprovar persistência
        audit_1.start_run("run_safety_1", force=False)
        self.assertEqual(self.spark.read.format("delta").load(audit_1.runs_path).count(), 1)

        # Caso 2: Path existente e válido -> inicializa normalmente preservando os dados (sem overwrite vazio)
        audit_2 = AuditManager(self.spark, warehouse_prefix=safety_warehouse)
        self.assertEqual(self.spark.read.format("delta").load(audit_2.runs_path).count(), 1)

        # Caso 3: Path existente fisicamente porém ilegível/inválido
        corrupt_warehouse = f"{self.isolated_warehouse}/corrupted_audit"
        corrupt_key = f"{self.test_s3_prefix}/warehouse/corrupted_audit/ingestion_runs/corrupted_file.txt"
        # Gravar arquivo texto plano no local onde deveria existir Delta Log
        self.s3_mgr.s3_client.put_object(
            Bucket=self.s3_mgr.bucket,
            Key=corrupt_key,
            Body=b"This is not a delta table"
        )

        # AuditManager deve falhar com RuntimeError ao tentar verificar a tabela corrompida existente
        with self.assertRaises(RuntimeError) as ctx:
            AuditManager(self.spark, warehouse_prefix=corrupt_warehouse)

        self.assertIn("Operação abortada para evitar perda de dados históricos", str(ctx.exception))

if __name__ == "__main__":
    unittest.main()
