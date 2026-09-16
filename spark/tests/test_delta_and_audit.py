"""
Testes de integração para gravação Delta Bronze e tabelas de auditoria.
"""
import os
import unittest
import tempfile
from transferegov.config import AppConfig, DatasetConfig
from transferegov.csv_processor import validate_and_count_csv
from transferegov.bronze_writer import get_spark_session, write_bronze_delta_table
from transferegov.audit import AuditManager

class TestDeltaAndAudit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = AppConfig()
        cls.spark = get_spark_session(cls.config)

    def setUp(self):
        self.temp_dir_path = "/data/staging/_test_tmp"
        os.makedirs(self.temp_dir_path, exist_ok=True)

    def tearDown(self):
        import shutil
        if os.path.exists(self.temp_dir_path):
            shutil.rmtree(self.temp_dir_path)

    def test_delta_write_preserves_string_type_and_leading_zeros(self):
        # CSV sintético com zeros à esquerda
        csv_path = os.path.join(self.temp_dir_path, "synthetic_prog.csv")
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write("ID_PROPOSTA;CODIGO;NOME\r\n000123;0045;Projeto Alpha\r\n000124;0046;Projeto Beta\r\n")

        val = validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")

        ds_cfg = DatasetConfig(
            id="test_synthetic",
            zip_file="test_synthetic.zip",
            member_file="synthetic_prog.csv",
            table_name="test_synthetic",
            delta_path="s3a://bronze/warehouse/_test_synthetic",
            raw_s3_prefix="s3://bronze/raw/transferegov/test_synthetic"
        )

        res = write_bronze_delta_table(
            spark=self.spark,
            csv_path=csv_path,
            header_columns=val["header_columns"],
            dataset_cfg=ds_cfg,
            run_id="run_test_zeros_1",
            source_file="test_synthetic.zip",
            source_sha256="dummyhash123",
            logical_row_count=val["logical_row_count"]
        )

        self.assertEqual(res["delta_output_rows"], 2)
        self.assertEqual(res["logical_input_rows"], 2)

        # Ler de volta do Delta e verificar que zeros à esquerda não foram perdidos
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

    def test_audit_idempotency(self):
        audit_mgr = AuditManager(self.spark)
        run_id = "run_test_audit_idemp_1"

        # Registrar início
        audit_mgr.start_run(run_id, force=False, control_info={"source_data_carga_raw": "16/09/2026 06:32:11"})

        # Registrar início novamente com o mesmo ID (simulando reexecução ou retry)
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
            "raw_s3_key": "raw/test/key",
            "delta_table_path": "s3a://bronze/warehouse/_test_ds",
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

if __name__ == "__main__":
    unittest.main()
