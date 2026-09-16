"""
Testes unitários para o processador de CSV e extrator seguro de ZIP.
"""
import os
import zipfile
import tempfile
import unittest
from transferegov.csv_processor import (
    validate_and_count_csv,
    extract_expected_member
)

class TestCsvProcessor(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_valid_csv_with_utf8_bom_and_semicolon(self):
        csv_path = os.path.join(self.temp_dir.name, "test_valid.csv")
        # Conteúdo com UTF-8 BOM (\xef\xbb\xbf), zeros à esquerda ("00123"),
        # campo multilinha ("Primeira linha\nSegunda linha") e campo vazio (;;)
        content = (
            "\ufeffID_PROPOSTA;CODIGO_PROGRAMA;DESCRICAO;VALOR_GLOBAL\r\n"
            "00123;PROG-01;\"Texto com quebra\r\nde linha interna\";150000.00\r\n"
            "00124;PROG-02;\"Texto simples\";\r\n"
        )
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        res = validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")
        self.assertEqual(res["num_columns"], 4)
        self.assertEqual(res["header_columns"], ["ID_PROPOSTA", "CODIGO_PROGRAMA", "DESCRICAO", "VALOR_GLOBAL"])
        # Deve contar exatamente 2 registros lógicos, mesmo com a quebra de linha interna
        self.assertEqual(res["logical_row_count"], 2)

    def test_strict_decoding_invalid_bytes(self):
        csv_path = os.path.join(self.temp_dir.name, "invalid_bytes.csv")
        # Escreve bytes inválidos para UTF-8 (ex: 0x80 solto não precedido de byte de liderança)
        with open(csv_path, "wb") as f:
            f.write(b"COL1;COL2\r\n\x80\x81;VALOR\r\n")

        with self.assertRaises(UnicodeDecodeError):
            validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")

    def test_empty_column_name_rejected(self):
        csv_path = os.path.join(self.temp_dir.name, "empty_col.csv")
        content = "COL1;;COL3\r\nVAL1;VAL2;VAL3\r\n"
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        with self.assertRaises(ValueError) as ctx:
            validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")
        self.assertIn("sem nome", str(ctx.exception))

    def test_duplicate_column_name_rejected(self):
        csv_path = os.path.join(self.temp_dir.name, "dup_col.csv")
        content = "ID_ITEM;NOME;ID_ITEM\r\n1;TESTE;1\r\n"
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        with self.assertRaises(ValueError) as ctx:
            validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")
        self.assertIn("duplicadas", str(ctx.exception))

    def test_technical_column_collision_rejected(self):
        csv_path = os.path.join(self.temp_dir.name, "collision.csv")
        content = "ID;NOME;__source_sha256\r\n1;TESTE;ABC\r\n"
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        with self.assertRaises(ValueError) as ctx:
            validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")
        self.assertIn("reservados", str(ctx.exception))

    def test_record_width_mismatch_rejected(self):
        csv_path = os.path.join(self.temp_dir.name, "mismatch.csv")
        content = "COL1;COL2\r\nVAL1;VAL2\r\nVAL1;VAL2;VAL3_EXTRA\r\n"
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(content)

        with self.assertRaises(ValueError) as ctx:
            validate_and_count_csv(csv_path, delimiter=";", encoding="utf-8-sig")
        self.assertIn("malformado", str(ctx.exception))

    def test_zip_extraction_valid(self):
        zip_path = os.path.join(self.temp_dir.name, "archive.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("esperado.csv", "ID;VAL\r\n1;A\r\n")
            zf.writestr("outro_arquivo.txt", "ignorar")

        dest_dir = os.path.join(self.temp_dir.name, "extracted")
        extracted = extract_expected_member(zip_path, "esperado.csv", dest_dir)
        self.assertTrue(os.path.exists(extracted))
        self.assertFalse(os.path.exists(os.path.join(dest_dir, "outro_arquivo.txt")))

    def test_zip_missing_expected_member(self):
        zip_path = os.path.join(self.temp_dir.name, "archive.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("arquivo_divergente.csv", "ID;VAL\r\n1;A\r\n")

        dest_dir = os.path.join(self.temp_dir.name, "extracted")
        with self.assertRaises(ValueError) as ctx:
            extract_expected_member(zip_path, "esperado.csv", dest_dir)
        self.assertIn("não encontrado", str(ctx.exception))

if __name__ == "__main__":
    unittest.main()
