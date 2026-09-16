"""
Testes unitários para download HTTP com streaming e armazenamento S3.
"""
import os
import hashlib
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from transferegov.http_downloader import download_file
from transferegov.s3_storage import S3StorageManager
from transferegov.config import StorageConfig

class TestDownloadAndS3(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_s3_key_naming(self):
        cfg = StorageConfig(
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
        mgr = S3StorageManager(cfg)
        key = mgr.build_raw_s3_key("siconv_convenio", "abc123hash", "siconv_convenio.zip")
        self.assertEqual(key, "raw/transferegov/siconv_convenio/sha256=abc123hash/siconv_convenio.zip")

    @patch("requests.Session.get")
    def test_download_success_with_sha256_and_part_promotion(self, mock_get):
        payload = b"TESTE_DE_CONTEUDO_CHUNK_STREAMING_12345"
        expected_sha = hashlib.sha256(payload).hexdigest()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Length": str(len(payload)), "ETag": '"etag123"'}
        mock_resp.iter_content.return_value = [payload[:10], payload[10:]]
        mock_get.return_value.__enter__.return_value = mock_resp

        dest_file = os.path.join(self.temp_dir.name, "output.zip")
        res = download_file("http://example.com/test.zip", dest_file, chunk_size=16)

        self.assertTrue(os.path.exists(dest_file))
        self.assertFalse(os.path.exists(dest_file + ".part"))
        self.assertEqual(res["sha256"], expected_sha)
        self.assertEqual(res["file_size_bytes"], len(payload))

    @patch("requests.Session.get")
    def test_download_sha_mismatch_cleans_part_and_raises(self, mock_get):
        payload = b"CONTEUDO_ORIGINAL"

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Length": str(len(payload))}
        mock_resp.iter_content.return_value = [payload]
        mock_get.return_value.__enter__.return_value = mock_resp

        dest_file = os.path.join(self.temp_dir.name, "mismatch.zip")
        with self.assertRaises(RuntimeError):
            download_file(
                "http://example.com/test.zip",
                dest_file,
                expected_sha256="hash_completamente_errado",
                max_retries=1
            )

        self.assertFalse(os.path.exists(dest_file))
        self.assertFalse(os.path.exists(dest_file + ".part"))

if __name__ == "__main__":
    unittest.main()
