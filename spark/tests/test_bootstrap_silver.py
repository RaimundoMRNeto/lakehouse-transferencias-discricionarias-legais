import unittest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

# Adiciona raiz do projeto ao PYTHONPATH para importar scripts.bootstrap_silver
root_dir = Path(__file__).resolve().parent.parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from scripts.bootstrap_silver import (
    normalize_location,
    get_database_location,
    database_exists,
    bootstrap_database,
)


class TestBootstrapSilver(unittest.TestCase):

    def test_normalize_location(self):
        self.assertEqual(normalize_location(None), "")
        self.assertEqual(normalize_location(""), "")
        self.assertEqual(normalize_location("   "), "")
        self.assertEqual(normalize_location("s3a://silver/warehouse"), "s3a://silver/warehouse")
        self.assertEqual(normalize_location("s3a://silver/warehouse/"), "s3a://silver/warehouse")
        self.assertEqual(normalize_location("  s3a://silver/warehouse/// "), "s3a://silver/warehouse")

    def test_get_database_location_success(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("Catalog Name", "spark_catalog"),
            ("Namespace Name", "silver"),
            ("Location", "s3a://silver/warehouse"),
            ("Owner", "airflow"),
        ]
        loc = get_database_location(mock_cur, "silver")
        self.assertEqual(loc, "s3a://silver/warehouse")
        mock_cur.execute.assert_called_once_with("DESCRIBE DATABASE EXTENDED silver")

    def test_get_database_location_missing_or_error(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("Catalog Name", "spark_catalog"),
            ("Namespace Name", "silver"),
        ]
        self.assertIsNone(get_database_location(mock_cur, "silver"))

        mock_cur.execute.side_effect = Exception("Schema not found")
        self.assertIsNone(get_database_location(mock_cur, "silver"))

    def test_database_exists(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [("bronze",), ("default",), ("silver",)]

        self.assertTrue(database_exists(mock_cur, "silver"))
        self.assertTrue(database_exists(mock_cur, "SILVER"))
        self.assertTrue(database_exists(mock_cur, "  silver  "))
        self.assertFalse(database_exists(mock_cur, "gold"))

    def test_bootstrap_database_case_a_creation(self):
        mock_cur = MagicMock()
        # Primeiro SHOW DATABASES não contém silver
        # Segundo fetchall para pós-criação DESCRIBE DATABASE EXTENDED retorna o location
        mock_cur.fetchall.side_effect = [
            [("bronze",), ("default",)],  # SHOW DATABASES (não existe)
            [("Location", "s3a://silver/warehouse")]  # DESCRIBE DATABASE EXTENDED pós-criação
        ]

        success, msg = bootstrap_database(mock_cur, "silver", "s3a://silver/warehouse")
        self.assertTrue(success)
        self.assertIn("criado com sucesso", msg)
        mock_cur.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://silver/warehouse'")

    def test_bootstrap_database_case_b_noop_exact_match(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [("bronze",), ("default",), ("silver",)],  # SHOW DATABASES (existe)
            [("Location", "s3a://silver/warehouse")]  # DESCRIBE DATABASE EXTENDED
        ]

        success, msg = bootstrap_database(mock_cur, "silver", "s3a://silver/warehouse")
        self.assertTrue(success)
        self.assertIn("NOOP idempotente", msg)

    def test_bootstrap_database_case_b_noop_trailing_slash(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [("bronze",), ("default",), ("silver",)],  # SHOW DATABASES (existe)
            [("Location", "s3a://silver/warehouse/")]  # Retorna com barra final
        ]

        # Location esperado sem barra final deve ser reconhecido como idêntico
        success, msg = bootstrap_database(mock_cur, "silver", "s3a://silver/warehouse")
        self.assertTrue(success)
        self.assertIn("NOOP idempotente", msg)

    def test_bootstrap_database_case_c_conflict(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.side_effect = [
            [("bronze",), ("default",), ("silver",)],  # SHOW DATABASES (existe)
            [("Location", "s3a://gold/warehouse/silver.db")]  # Location incorreto!
        ]

        success, msg = bootstrap_database(mock_cur, "silver", "s3a://silver/warehouse")
        self.assertFalse(success)
        self.assertIn("CONFLITO CRÍTICO DE LOCATION", msg)

    def test_bootstrap_database_empty_location(self):
        mock_cur = MagicMock()
        success, msg = bootstrap_database(mock_cur, "silver", "   ")
        self.assertFalse(success)
        self.assertIn("não pode ser vazio", msg)


if __name__ == "__main__":
    unittest.main()
