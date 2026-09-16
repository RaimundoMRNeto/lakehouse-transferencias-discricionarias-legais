"""
Módulo de armazenamento S3/MinIO para preservação RAW dos arquivos ZIP originais.
"""
import os
import hashlib
import logging
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError
from transferegov.config import StorageConfig

logger = logging.getLogger(__name__)

class S3StorageManager:
    def __init__(self, config: StorageConfig):
        self.config = config
        self.bucket = config.s3_bucket_bronze
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=config.s3_endpoint,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            region_name=config.s3_region
        )

    def build_raw_s3_key(self, dataset_id: str, sha256: str, file_name: str) -> str:
        """Gera a chave S3 padronizada para o arquivo RAW."""
        return f"{self.config.raw_prefix}/{dataset_id}/sha256={sha256}/{file_name}"

    def get_s3_uri(self, key: str) -> str:
        """Retorna o URI s3:// correspondente à chave."""
        return f"s3://{self.bucket}/{key}"

    def get_s3a_uri(self, key: str) -> str:
        """Retorna o URI s3a:// correspondente à chave."""
        return f"s3a://{self.bucket}/{key}"

    def upload_raw_zip(
        self,
        local_file_path: str,
        dataset_id: str,
        calculated_sha256: str
    ) -> Dict[str, Any]:
        """
        Preserva o arquivo ZIP original byte a byte em:
        s3://bronze/raw/transferegov/<dataset>/sha256=<hash>/<arquivo>.zip

        Garante imutabilidade: impede sobrescrita com bytes divergentes.
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Arquivo local não encontrado: {local_file_path}")

        file_name = os.path.basename(local_file_path)
        file_size = os.path.getsize(local_file_path)
        s3_key = self.build_raw_s3_key(dataset_id, calculated_sha256, file_name)
        s3_uri = self.get_s3_uri(s3_key)

        # Verificar se o objeto já existe
        try:
            head_resp = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            existing_size = head_resp.get("ContentLength")
            existing_meta = head_resp.get("Metadata", {})
            existing_sha = existing_meta.get("sha256")

            # Se tamanho e hash coincidirem, o objeto já está seguramente armazenado
            if existing_size == file_size and (not existing_sha or existing_sha == calculated_sha256):
                logger.info(f"Arquivo RAW já existente no S3 ({s3_uri}) com tamanho e hash idênticos. Reutilizando.")
                return {
                    "s3_uri": s3_uri,
                    "s3_key": s3_key,
                    "sha256": calculated_sha256,
                    "file_size_bytes": file_size,
                    "status": "ALREADY_EXISTS"
                }
            else:
                raise ValueError(
                    f"Conflito crítico de integridade: a chave S3 {s3_key} já existe, "
                    f"mas com tamanho {existing_size} bytes (esperado {file_size}). Sobrescrita proibida."
                )
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code != "404":
                raise RuntimeError(f"Erro ao consultar objeto no S3 ({s3_key}): {e}")

        # Objeto não existe, realizar o upload
        logger.info(f"Enviando arquivo RAW para {s3_uri} ({file_size} bytes)...")
        with open(local_file_path, "rb") as f:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=f,
                Metadata={"sha256": calculated_sha256}
            )

        # Confirmar envio e tamanho
        confirm_resp = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
        confirmed_size = confirm_resp.get("ContentLength")
        if confirmed_size != file_size:
            raise RuntimeError(
                f"Falha na confirmação de envio para {s3_uri}: "
                f"tamanho confirmado ({confirmed_size}) difere do local ({file_size})."
            )

        logger.info(f"Upload confirmado com sucesso em {s3_uri}.")
        return {
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "sha256": calculated_sha256,
            "file_size_bytes": file_size,
            "status": "UPLOADED"
        }

    def object_exists(self, key: str) -> bool:
        """
        Verifica se um objeto existe no bucket S3.
        Contrato:
        - Objeto existe: retorna True
        - 404 / NoSuchKey / NotFound: retorna False
        - Erros de autenticação, permissão, timeout, 500, falha de rede ou outros erros S3:
          propaga exceção imediatamente (nunca converte falhas de infraestrutura em False).
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            error_code = str(exc.response.get("Error", {}).get("Code", ""))
            http_status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if error_code in ("404", "NoSuchKey", "NotFound") or http_status == 404:
                return False
            logger.error(f"Erro S3 inesperado ({error_code}) ao verificar existência de '{key}': {exc}")
            raise
        except Exception as exc:
            logger.error(f"Falha de infraestrutura ao acessar S3 para chave '{key}': {exc}")
            raise

    def raw_key_exists(self, key: str) -> bool:
        """Alias explícito para verificação de chaves RAW com o mesmo contrato de object_exists."""
        return self.object_exists(key)

    def check_raw_exists(self, dataset_id: str, sha256: str, file_name: str) -> bool:
        """Verifica se a chave RAW existe no S3 utilizando o contrato estrito de object_exists."""
        s3_key = self.build_raw_s3_key(dataset_id, sha256, file_name)
        return self.object_exists(s3_key)

    def list_dataset_raw_objects(self, dataset_id: str) -> List[Dict[str, Any]]:
        """Lista todos os objetos RAW sob o prefixo do dataset."""
        prefix = f"{self.config.raw_prefix}/{dataset_id}/"
        results = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                results.append({
                    "key": item["Key"],
                    "size": item["Size"],
                    "last_modified": item["LastModified"]
                })
        return results

    def delete_exact_raw_key(self, s3_key: str) -> bool:
        """
        Exclui estritamente uma chave exata no S3/MinIO.
        Nunca executa exclusão genérica ou recursiva.
        """
        if not s3_key.startswith(self.config.raw_prefix):
            raise ValueError(f"Chave S3 não permitida para exclusão fora do prefixo RAW: {s3_key}")

        logger.info(f"Excluindo chave S3 controlada: {s3_key}")
        self.s3_client.delete_object(Bucket=self.bucket, Key=s3_key)

        # Confirmar exclusão
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return False # Ainda existe
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return True
            raise
