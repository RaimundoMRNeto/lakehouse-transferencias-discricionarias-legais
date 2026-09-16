"""
Módulo de download HTTP seguro e resiliente com streaming, cálculo de SHA-256 e arquivos parciais.
"""
import os
import shutil
import hashlib
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

def get_disk_free_bytes(path: str) -> int:
    """Retorna espaço livre em bytes no diretório especificado."""
    target_dir = path
    while not os.path.exists(target_dir):
        parent = os.path.dirname(target_dir)
        if parent == target_dir:
            break
        target_dir = parent
    total, used, free = shutil.disk_usage(target_dir)
    return free

def download_file(
    url: str,
    destination_path: str,
    expected_sha256: Optional[str] = None,
    chunk_size: int = 65536,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    timeout: int = 120
) -> Dict[str, Any]:
    """
    Realiza o download de uma URL com:
    - Gravação em arquivo .part
    - Cálculo contínuo de SHA-256 sem carregar todo o arquivo na memória
    - Retries com backoff exponencial
    - Validação de status HTTP 200
    - Validação de integridade e promoção atômica do arquivo final
    """
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    part_path = destination_path + ".part"

    # Verificar espaço em disco mínimo (pelo menos 500 MB de margem de segurança)
    free_bytes = get_disk_free_bytes(destination_path)
    min_required = 500 * 1024 * 1024
    if free_bytes < min_required:
        raise OSError(f"Espaço em disco insuficiente em {destination_path}. Livre: {free_bytes} bytes, Requerido mínimo: {min_required} bytes.")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) LakehouseIngestion/1.0"
    }

    last_exception = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Iniciando download de {url} (tentativa {attempt}/{max_retries})")
            session = requests.Session()

            with session.get(url, headers=headers, stream=True, timeout=timeout) as response:
                if response.status_code != 200:
                    raise requests.HTTPError(f"HTTP Status {response.status_code} ao baixar {url}")

                content_length = response.headers.get("Content-Length")
                if content_length:
                    expected_bytes = int(content_length)
                    if free_bytes < (expected_bytes * 2):
                        raise OSError(f"Espaço em disco insuficiente para o download ({expected_bytes} bytes necessários).")

                hasher = hashlib.sha256()
                bytes_written = 0

                with open(part_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            hasher.update(chunk)
                            bytes_written += len(chunk)

                if bytes_written == 0:
                    raise ValueError(f"Download retornou 0 bytes para {url}")

                calculated_sha256 = hasher.hexdigest()

                if expected_sha256 and calculated_sha256.lower() != expected_sha256.lower():
                    raise ValueError(
                        f"Divergência de SHA-256 para {url}. "
                        f"Esperado: {expected_sha256}, Calculado: {calculated_sha256}"
                    )

                # Promoção atômica do arquivo temporário
                if os.path.exists(destination_path):
                    os.remove(destination_path)
                os.rename(part_path, destination_path)

                retrieved_at_utc = datetime.now(timezone.utc).isoformat()
                etag = response.headers.get("ETag", "").strip('"')
                last_modified = response.headers.get("Last-Modified", "")

                logger.info(f"Download concluído com sucesso: {destination_path} ({bytes_written} bytes, SHA256: {calculated_sha256})")

                return {
                    "file_path": destination_path,
                    "sha256": calculated_sha256,
                    "file_size_bytes": bytes_written,
                    "etag": etag,
                    "last_modified": last_modified,
                    "retrieved_at_utc": retrieved_at_utc
                }

        except Exception as exc:
            last_exception = exc
            logger.warning(f"Falha no download de {url} na tentativa {attempt}: {exc}")
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                logger.info(f"Aguardando {sleep_time:.1f}s antes da próxima tentativa...")
                time.sleep(sleep_time)

    raise RuntimeError(f"Download falhou após {max_retries} tentativas para {url}: {last_exception}")
