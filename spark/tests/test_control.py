"""
Testes unitários para o módulo de controle de data_carga.
"""
import unittest
from datetime import datetime
from transferegov.control import parse_data_carga, check_if_no_change_eligible

class TestControlDataCarga(unittest.TestCase):

    def test_valid_data_carga(self):
        raw = "16/09/2026 06:32:11"
        cleaned, dt = parse_data_carga(raw)
        self.assertEqual(cleaned, "16/09/2026 06:32:11")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 9)
        self.assertEqual(dt.day, 16)
        self.assertEqual(dt.hour, 6)
        self.assertEqual(dt.minute, 32)
        self.assertEqual(dt.second, 11)

    def test_empty_data_carga(self):
        with self.assertRaises(ValueError):
            parse_data_carga("   ")

    def test_invalid_calendar_date(self):
        # 31 de fevereiro é data inexistente
        with self.assertRaises(ValueError):
            parse_data_carga("31/02/2026 10:00:00")

    def test_invalid_format(self):
        # Formato ISO não é o publicado no controle oficial
        with self.assertRaises(ValueError):
            parse_data_carga("2026-09-16 06:32:11")

    def test_no_change_when_date_equal_and_state_valid(self):
        last_success = {
            "source_data_carga_raw_final": "16/09/2026 06:32:11",
            "status": "SUCCESS"
        }
        current_control = {
            "source_data_carga_raw": "16/09/2026 06:32:11"
        }
        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=lambda: (True, "OK"),
            force=False
        )
        self.assertTrue(is_no_change)
        self.assertIn("idêntica", reason)

    def test_no_change_rejected_when_date_different(self):
        last_success = {
            "source_data_carga_raw_final": "15/09/2026 06:00:00",
            "status": "SUCCESS"
        }
        current_control = {
            "source_data_carga_raw": "16/09/2026 06:32:11"
        }
        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=lambda: (True, "OK"),
            force=False
        )
        self.assertFalse(is_no_change)
        self.assertIn("alterada", reason)

    def test_no_change_rejected_when_force_true(self):
        last_success = {
            "source_data_carga_raw_final": "16/09/2026 06:32:11",
            "status": "SUCCESS"
        }
        current_control = {
            "source_data_carga_raw": "16/09/2026 06:32:11"
        }
        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=lambda: (True, "OK"),
            force=True
        )
        self.assertFalse(is_no_change)
        self.assertIn("force=True", reason)

    def test_no_change_rejected_when_state_invalid(self):
        last_success = {
            "source_data_carga_raw_final": "16/09/2026 06:32:11",
            "status": "SUCCESS"
        }
        current_control = {
            "source_data_carga_raw": "16/09/2026 06:32:11"
        }
        # Simula falha na integridade local (ex: tabela ausente)
        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=last_success,
            current_control=current_control,
            integrity_verifier_func=lambda: (False, "Tabela siconv_convenio ausente"),
            force=False
        )
        self.assertFalse(is_no_change)
        self.assertIn("estado local está incompleto", reason)

    def test_no_change_rejected_on_first_run(self):
        current_control = {
            "source_data_carga_raw": "16/09/2026 06:32:11"
        }
        is_no_change, reason = check_if_no_change_eligible(
            last_successful_run=None,
            current_control=current_control,
            integrity_verifier_func=lambda: (True, "OK"),
            force=False
        )
        self.assertFalse(is_no_change)
        self.assertIn("Primeira execução", reason)

if __name__ == "__main__":
    unittest.main()
