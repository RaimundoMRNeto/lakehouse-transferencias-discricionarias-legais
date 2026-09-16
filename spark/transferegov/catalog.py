"""
Módulo de interação com o catálogo Spark Thrift Server / Hive Metastore.
"""
import logging
from typing import List, Tuple
from pyhive import hive

logger = logging.getLogger(__name__)

class CatalogManager:
    def __init__(self, host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
        self.host = host
        self.port = port
        self.user = user

    def _get_connection(self):
        return hive.Connection(host=self.host, port=self.port, username=self.user)

    def ensure_schema(self, schema_name: str = "bronze", location: str = "s3a://bronze/warehouse"):
        """Garante a existência do schema apontando para a location explícita."""
        logger.info(f"Assegurando schema '{schema_name}' com LOCATION '{location}' no Thrift Server...")
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name} LOCATION '{location}'")
            cursor.close()
        finally:
            conn.close()

    def register_delta_table(self, schema_name: str, table_name: str, delta_location: str):
        """
        Registra a tabela externa Delta no Thrift Server de forma idempotente:
        - Se não existir, executa CREATE TABLE ... USING DELTA LOCATION ...
        - Se já existir, executa REFRESH TABLE para sincronizar metadados sem recriar
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES IN {schema_name} LIKE '{table_name}'")
            existing = cursor.fetchall()

            if not existing:
                logger.info(f"Registrando tabela externa Delta {schema_name}.{table_name} em {delta_location}...")
                create_sql = (
                    f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} "
                    f"USING DELTA LOCATION '{delta_location}'"
                )
                cursor.execute(create_sql)
            else:
                logger.info(f"Tabela {schema_name}.{table_name} já existe no catálogo. Executando REFRESH TABLE...")
                cursor.execute(f"REFRESH TABLE {schema_name}.{table_name}")

            cursor.close()
        finally:
            conn.close()

    def query_count(self, schema_name: str, table_name: str) -> int:
        """Executa SELECT COUNT(*) no Thrift Server para validar a leitura externa."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            row = cursor.fetchone()
            cursor.close()
            return int(row[0]) if row else 0
        finally:
            conn.close()

    def list_tables(self, schema_name: str = "bronze") -> List[str]:
        """Lista as tabelas registradas no schema."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES IN {schema_name}")
            rows = cursor.fetchall()
            cursor.close()
            # Retorna lista de nomes de tabelas (segunda coluna na tupla)
            return [r[1] for r in rows] if rows else []
        finally:
            conn.close()
