"""
Testes unitários para a política de retenção RAW (atual + anterior = 2 versões).
"""
import unittest
from unittest.mock import MagicMock
from transferegov.retention import execute_retention

class TestRetentionPolicy(unittest.TestCase):

    def test_retention_simulation_a_b_c(self):
        """
        Simula a linha do tempo A -> B -> C:
        - Versão A: ativada no Run 1 (hash A)
        - Versão B: ativada no Run 2 (hash B)
        - Versão C: ativada no Run 3 (hash C)
        A política de 2 versões deve proteger C (atual) e B (anterior), e marcar A como exclusão.
        """
        distinct_successful_hashes = ["hash_C", "hash_B", "hash_A"]
        protected_hashes = distinct_successful_hashes[:2]  # ["hash_C", "hash_B"]

        self.assertEqual(protected_hashes, ["hash_C", "hash_B"])
        self.assertNotIn("hash_A", protected_hashes)

        # Simulação de objetos físicos no storage
        physical_keys = [
            "raw/transferegov/siconv_programa/sha256=hash_C/siconv_programa.zip",
            "raw/transferegov/siconv_programa/sha256=hash_B/siconv_programa.zip",
            "raw/transferegov/siconv_programa/sha256=hash_A/siconv_programa.zip"
        ]

        candidates = [k for k in physical_keys if not any(h in k for h in protected_hashes)]
        protected = [k for k in physical_keys if any(h in k for h in protected_hashes)]

        self.assertEqual(len(protected), 2)
        self.assertEqual(len(candidates), 1)
        self.assertIn("sha256=hash_A", candidates[0])

    def test_reused_hash_does_not_evict_previous(self):
        """
        Simula repetição de hash:
        - Run 1 ativou hash_A
        - Run 2 ativou hash_B
        - Run 3 reexecutou com hash_B (mesmo conteúdo)
        A lista de hashes distintos ativados permanece [hash_B, hash_A].
        Portanto, tanto hash_B quanto hash_A continuam protegidos.
        """
        raw_manifest_history = ["hash_B", "hash_B", "hash_A"]
        distinct_successful = []
        for h in raw_manifest_history:
            if h not in distinct_successful:
                distinct_successful.append(h)

        protected = distinct_successful[:2]
        self.assertEqual(protected, ["hash_B", "hash_A"])
        # Nenhum deve ser excluído
        self.assertTrue("hash_A" in protected and "hash_B" in protected)

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

if __name__ == "__main__":
    unittest.main()
