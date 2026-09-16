"""
Testes unitários para a política de retenção RAW (atual + anterior = 2 versões).
Testa a lógica do planejador real (plan_dataset_retention) e do executor (execute_retention).
"""
import unittest
from unittest.mock import MagicMock
from transferegov.retention import plan_dataset_retention, execute_retention

class TestRetentionPolicy(unittest.TestCase):

    def test_plan_retention_a_b_c_analytical(self):
        """
        Simula a linha do tempo A -> B -> C para dataset analítico siconv_programa:
        - Versão A: ativada no Run 1 (hash A)
        - Versão B: ativada no Run 2 (hash B)
        - Versão C: ativada no Run 3 (hash C)
        A política de 2 versões deve proteger C (atual) e B (anterior), e marcar A como exclusão.
        """
        manifests = [
            {"source_sha256": "hash_C", "raw_s3_key": "raw/transferegov/siconv_programa/sha256=hash_C/siconv_programa.zip", "ingested_at_utc": "2026-09-16T15:00:00Z"},
            {"source_sha256": "hash_B", "raw_s3_key": "raw/transferegov/siconv_programa/sha256=hash_B/siconv_programa.zip", "ingested_at_utc": "2026-09-16T14:00:00Z"},
            {"source_sha256": "hash_A", "raw_s3_key": "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip", "ingested_at_utc": "2026-09-16T13:00:00Z"},
        ]
        physical_keys = [
            "raw/transferegov/siconv_programa/sha256=hash_C/siconv_programa.zip",
            "raw/transferegov/siconv_programa/sha256=hash_B/siconv_programa.zip",
            "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip",
        ]

        plan = plan_dataset_retention(
            dataset_id="siconv_programa",
            successful_manifests=manifests,
            physical_keys=physical_keys,
            raw_versions_to_keep=2
        )

        self.assertEqual(plan["protected_hashes"], ["hash_C", "hash_B"])
        self.assertIn("raw/transferegov/siconv_programa/sha256=hash_C/siconv_programa.zip", plan["protected_keys"])
        self.assertIn("raw/transferegov/siconv_programa/sha256=hash_B/siconv_programa.zip", plan["protected_keys"])
        self.assertEqual(plan["deletion_candidates"], ["raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip"])

    def test_plan_retention_control_data_carga(self):
        """
        Valida que o planner real protege corretamente atual + anterior para data_carga_siconv:
        - Controle A (Run 1, SUCCESS)
        - Controle B (Run 2, SUCCESS)
        - Controle C (Run 3, SUCCESS)
        Protegidos: B e C. Candidato à exclusão: A.
        """
        manifests = [
            {"source_sha256": "hash_ctrl_C", "raw_s3_key": "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_C/data_carga_siconv.zip", "ingested_at_utc": "2026-09-16T15:00:00Z"},
            {"source_sha256": "hash_ctrl_B", "raw_s3_key": "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_B/data_carga_siconv.zip", "ingested_at_utc": "2026-09-16T14:00:00Z"},
            {"source_sha256": "hash_ctrl_A", "raw_s3_key": "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_A/data_carga_siconv.zip", "ingested_at_utc": "2026-09-16T13:00:00Z"},
        ]
        physical_keys = [
            "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_C/data_carga_siconv.zip",
            "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_B/data_carga_siconv.zip",
            "raw/transferegov/data_carga_siconv/sha256=hash_ctrl_A/data_carga_siconv.zip",
        ]

        plan = plan_dataset_retention(
            dataset_id="data_carga_siconv",
            successful_manifests=manifests,
            physical_keys=physical_keys,
            raw_versions_to_keep=2
        )

        self.assertEqual(plan["protected_hashes"], ["hash_ctrl_C", "hash_ctrl_B"])
        self.assertEqual(len(plan["protected_keys"]), 2)
        self.assertEqual(plan["deletion_candidates"], ["raw/transferegov/data_carga_siconv/sha256=hash_ctrl_A/data_carga_siconv.zip"])

    def test_plan_retention_repetition_b_b_preserves_previous(self):
        """
        Simula repetição de hash B -> B:
        - Run 1 ativou hash_A
        - Run 2 ativou hash_B
        - Run 3 reexecutou com hash_B
        Hashes distintos: [hash_B, hash_A].
        Tanto hash_B quanto hash_A devem permanecer protegidos; zero exclusões.
        """
        manifests = [
            {"source_sha256": "hash_B", "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=hash_B/siconv_convenio.zip", "ingested_at_utc": "2026-09-16T15:00:00Z"},
            {"source_sha256": "hash_B", "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=hash_B/siconv_convenio.zip", "ingested_at_utc": "2026-09-16T14:00:00Z"},
            {"source_sha256": "hash_A", "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=hash_A/siconv_convenio.zip", "ingested_at_utc": "2026-09-16T13:00:00Z"},
        ]
        physical_keys = [
            "raw/transferegov/siconv_convenio/sha256=hash_B/siconv_convenio.zip",
            "raw/transferegov/siconv_convenio/sha256=hash_A/siconv_convenio.zip",
        ]

        plan = plan_dataset_retention(
            dataset_id="siconv_convenio",
            successful_manifests=manifests,
            physical_keys=physical_keys,
            raw_versions_to_keep=2
        )

        self.assertEqual(plan["protected_hashes"], ["hash_B", "hash_A"])
        self.assertEqual(len(plan["protected_keys"]), 2)
        self.assertEqual(len(plan["deletion_candidates"]), 0)

    def test_plan_retention_ignores_failed_run_hashes(self):
        """
        Objeto físico pertencente a execução FAILED (não ativada no manifesto de sucesso)
        deve ser classificado como candidato à exclusão e JAMAIS protegido.
        """
        manifests = [
            {"source_sha256": "hash_valid_1", "raw_s3_key": "raw/transferegov/siconv_proposta/sha256=hash_valid_1/siconv_proposta.zip", "ingested_at_utc": "2026-09-16T15:00:00Z"},
        ]
        physical_keys = [
            "raw/transferegov/siconv_proposta/sha256=hash_valid_1/siconv_proposta.zip",
            "raw/transferegov/siconv_proposta/sha256=hash_failed_run/siconv_proposta.zip",
        ]

        plan = plan_dataset_retention(
            dataset_id="siconv_proposta",
            successful_manifests=manifests,
            physical_keys=physical_keys,
            raw_versions_to_keep=2
        )

        self.assertEqual(plan["protected_hashes"], ["hash_valid_1"])
        self.assertIn("raw/transferegov/siconv_proposta/sha256=hash_valid_1/siconv_proposta.zip", plan["protected_keys"])
        self.assertIn("raw/transferegov/siconv_proposta/sha256=hash_failed_run/siconv_proposta.zip", plan["deletion_candidates"])

    def test_plan_retention_unrecognized_hash_object(self):
        """
        Objeto no bucket sem o padrão sha256=<hash> deve ser marcado para exclusão.
        """
        manifests = [
            {"source_sha256": "hash_valid", "raw_s3_key": "raw/transferegov/siconv_convenio/sha256=hash_valid/siconv_convenio.zip", "ingested_at_utc": "2026-09-16T15:00:00Z"},
        ]
        physical_keys = [
            "raw/transferegov/siconv_convenio/sha256=hash_valid/siconv_convenio.zip",
            "raw/transferegov/siconv_convenio/orphaned_temp_file.tmp",
        ]

        plan = plan_dataset_retention(
            dataset_id="siconv_convenio",
            successful_manifests=manifests,
            physical_keys=physical_keys,
            raw_versions_to_keep=2
        )

        self.assertIn("raw/transferegov/siconv_convenio/orphaned_temp_file.tmp", plan["deletion_candidates"])

    def test_dry_run_executes_zero_deletions(self):
        """Verifica que dry-run lista os candidatos sem efetuar nenhuma exclusão no S3."""
        mock_s3 = MagicMock()
        mock_audit = MagicMock()

        plan = {
            "deletion_candidates": [
                "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip"
            ]
        }

        result = execute_retention(plan, mock_s3, mock_audit, dry_run=True)

        self.assertEqual(result["status"], "DRY_RUN")
        self.assertEqual(len(result["deleted_keys"]), 0)
        self.assertEqual(len(result["planned_deletions"]), 1)
        mock_s3.delete_exact_raw_key.assert_not_called()
        mock_audit.update_retention_records.assert_not_called()

    def test_real_execution_deletes_exact_keys(self):
        """Verifica que execução real chama delete_exact_raw_key e atualiza o manifesto."""
        mock_s3 = MagicMock()
        mock_s3.delete_exact_raw_key.return_value = True
        mock_audit = MagicMock()

        target_key = "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip"
        plan = {
            "deletion_candidates": [target_key]
        }

        result = execute_retention(plan, mock_s3, mock_audit, dry_run=False)

        self.assertEqual(result["status"], "EXECUTED")
        self.assertEqual(result["deleted_keys"], [target_key])
        mock_s3.delete_exact_raw_key.assert_called_once_with(target_key)
        mock_audit.update_retention_records.assert_called_once_with([target_key])

    def test_execution_partial_failure_handling(self):
        """
        Verifica que em caso de falha de exclusão de uma das chaves:
        - O status retornado é PARTIAL_FAILURE
        - Apenas a chave confirmada como excluída é atualizada na auditoria
        - A execução não é abortada abruptamente
        """
        mock_s3 = MagicMock()
        # Primeira chave tem sucesso, segunda falha (retorna False)
        mock_s3.delete_exact_raw_key.side_effect = [True, False]
        mock_audit = MagicMock()

        key1 = "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip"
        key2 = "raw/transferegov/siconv_proposta/sha256=hash_OLD/siconv_proposta.zip"
        plan = {
            "deletion_candidates": [key1, key2]
        }

        result = execute_retention(plan, mock_s3, mock_audit, dry_run=False)

        self.assertEqual(result["status"], "PARTIAL_FAILURE")
        self.assertEqual(result["deleted_keys"], [key1])
        self.assertEqual(result["failed_keys"], [key2])
        # Atualiza apenas key1 no manifesto
        mock_audit.update_retention_records.assert_called_once_with([key1])

if __name__ == "__main__":
    unittest.main()
