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


class TestControlContractValidation(unittest.TestCase):
    """Testes estritos de contrato para o arquivo CSV data_carga_siconv."""

    def setUp(self):
        import tempfile
        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.test_dir.cleanup()

    def _write_file(self, filename: str, content: str, encoding: str = "utf-8-sig") -> str:
        import os
        path = os.path.join(self.test_dir.name, filename)
        with open(path, "w", encoding=encoding, newline="") as f:
            f.write(content)
        return path

    def test_valid_single_column_single_row(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("valid.csv", "data_carga\r\n16/09/2026 06:32:11\r\n")
        raw, dt = validate_and_parse_control_csv(path)
        self.assertEqual(raw, "16/09/2026 06:32:11")
        self.assertEqual(dt.year, 2026)

    def test_reject_extra_column_in_header(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("extra_col.csv", "data_carga;coluna_extra\r\n16/09/2026 06:32:11;extra\r\n")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("adicional", str(ctx.exception))

    def test_reject_duplicate_columns_in_header(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("dup_col.csv", "data_carga;data_carga\r\n16/09/2026 06:32:11;16/09/2026 06:32:11\r\n")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("duplicada", str(ctx.exception))

    def test_reject_missing_expected_column(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("missing_col.csv", "outra_coluna\r\n16/09/2026 06:32:11\r\n")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("ausente", str(ctx.exception))

    def test_reject_empty_file_zero_lines(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("empty.csv", "")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("vazio", str(ctx.exception))

    def test_reject_header_only_zero_data_rows(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("header_only.csv", "data_carga\r\n")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("zero registros", str(ctx.exception))

    def test_reject_two_or_more_data_rows(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file(
            "multiple_rows.csv",
            "data_carga\r\n16/09/2026 06:32:11\r\n16/09/2026 07:00:00\r\n"
        )
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("duas ou mais linhas", str(ctx.exception))

    def test_reject_incompatible_row_width(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file(
            "incompat_width.csv",
            "data_carga\r\n16/09/2026 06:32:11;valor_extra\r\n"
        )
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("largura incompatível", str(ctx.exception))

    def test_reject_impossible_calendar_date(self):
        from transferegov.control import validate_and_parse_control_csv
        path = self._write_file("bad_date.csv", "data_carga\r\n31/02/2026 12:00:00\r\n")
        with self.assertRaises(ValueError) as ctx:
            validate_and_parse_control_csv(path)
        self.assertIn("não calendarizável", str(ctx.exception))

    def test_reject_invalid_encoding(self):
        from transferegov.control import validate_and_parse_control_csv
        import os
        path = os.path.join(self.test_dir.name, "bad_enc.csv")
        # Bytes ISO-8859-1 com caractere inválido para UTF-8 estrito
        with open(path, "wb") as f:
            f.write(b"data_carga\r\n16/09/2026 \xe7\xe3 06:32:11\r\n")
        with self.assertRaises(Exception):
            validate_and_parse_control_csv(path)


class TestTemporalConsistencyValidation(unittest.TestCase):
    """Testes de consistência temporal entre controle_inicial e controle_final (Finding 1)."""

    def setUp(self):
        import tempfile
        from unittest.mock import MagicMock
        from transferegov.config import AppConfig

        self.temp_dir = tempfile.TemporaryDirectory()
        self.config = AppConfig()
        self.config.storage.local_staging_dir = self.temp_dir.name

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_validate_global_fails_when_control_differs_a_vs_b(self):
        """
        Simula fonte mudando durante a execução da carga:
        controle_inicial = A, controle_final = B
        Comprova que run_validate_global falha e registra status FAILED.
        """
        import os
        import json
        from unittest.mock import MagicMock
        from ingest_transferegov import run_validate_global

        run_id = "run_test_temporal_diff"
        run_staging = os.path.join(self.config.storage.local_staging_dir, run_id)
        os.makedirs(run_staging, exist_ok=True)

        ctrl_init = {"source_data_carga_raw": "16/09/2026 06:00:00", "sha256": "sha_A"}
        ctrl_final = {"source_data_carga_raw": "16/09/2026 07:00:00", "sha256": "sha_B"}

        with open(os.path.join(run_staging, "control_initial.json"), "w") as f:
            json.dump(ctrl_init, f)
        with open(os.path.join(run_staging, "control_final.json"), "w") as f:
            json.dump(ctrl_final, f)

        mock_audit = MagicMock()

        with self.assertRaises(RuntimeError) as ctx:
            run_validate_global(
                config=self.config,
                run_id=run_id,
                audit_mgr=mock_audit
            )

        self.assertIn("Inconsistência temporal", str(ctx.exception))
        mock_audit.finish_run.assert_called_once()
        args, kwargs = mock_audit.finish_run.call_args
        self.assertEqual(kwargs.get("status"), "FAILED")
        self.assertEqual(kwargs.get("successful_datasets"), 0)
        mock_audit.record_control_manifest.assert_not_called()

    def test_validate_global_succeeds_when_control_identical_a_vs_a(self):
        """
        Simula consistência temporal preservada:
        controle_inicial = A, controle_final = A
        Comprova que run_validate_global passa, ativa o controle no manifesto e finaliza com SUCCESS.
        """
        import os
        import json
        from unittest.mock import MagicMock
        from ingest_transferegov import run_validate_global

        run_id = "run_test_temporal_equal"
        run_staging = os.path.join(self.config.storage.local_staging_dir, run_id)
        os.makedirs(run_staging, exist_ok=True)

        ctrl_init = {"source_data_carga_raw": "16/09/2026 06:00:00", "sha256": "sha_A", "dataset_id": "data_carga_siconv"}
        ctrl_final = {"source_data_carga_raw": "16/09/2026 06:00:00", "sha256": "sha_A", "dataset_id": "data_carga_siconv"}

        with open(os.path.join(run_staging, "control_initial.json"), "w") as f:
            json.dump(ctrl_init, f)
        with open(os.path.join(run_staging, "control_final.json"), "w") as f:
            json.dump(ctrl_final, f)

        mock_audit = MagicMock()
        mock_spark = MagicMock()
        mock_audit.spark = mock_spark

        # Simular 4 manifestos de sucesso para os 4 datasets analíticos
        mock_spark.read.format.return_value.load.return_value.filter.return_value.collect.return_value = [
            {"status": "SUCCESS"}
        ] * 4

        success = run_validate_global(
            config=self.config,
            run_id=run_id,
            audit_mgr=mock_audit
        )

        self.assertTrue(success)
        mock_audit.record_control_manifest.assert_called_once_with(run_id, ctrl_final)
        mock_audit.finish_run.assert_called_once()
        args, kwargs = mock_audit.finish_run.call_args
        self.assertEqual(kwargs.get("status"), "SUCCESS")
        self.assertEqual(kwargs.get("successful_datasets"), 4)

if __name__ == "__main__":
    unittest.main()
