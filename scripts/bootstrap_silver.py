#!/usr/bin/env python3
"""
Script de bootstrap e governança do schema Silver (R3-B.1).
Garante que o schema (database) exista no Spark Catalog apontando
estritamente para o bucket e path corretos no S3/MinIO.

Casos tratados:
- Caso A (Schema inexistente): Cria o database com LOCATION esperado e valida.
- Caso B (Schema existente com location correto): NOOP idempotente, sai com sucesso.
- Caso C (Schema existente com location divergente): Aborta com erro (exit code 1)
  sem dropar nem alterar o catálogo.
"""
import sys
import argparse
import logging
from typing import Optional, Tuple
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bootstrap_silver")


def normalize_location(loc: Optional[str]) -> str:
    """Normaliza URI de storage removendo barras finais e espaços."""
    if not loc:
        return ""
    return str(loc).strip().rstrip("/")


def get_database_location(cur, schema: str) -> Optional[str]:
    """Consulta o DESCRIBE DATABASE EXTENDED e extrai o campo Location."""
    try:
        cur.execute(f"DESCRIBE DATABASE EXTENDED {schema}")
        rows = cur.fetchall()
        for row in rows:
            if len(row) >= 2 and str(row[0]).strip().lower() == "location":
                return str(row[1]).strip()
    except Exception as e:
        logger.debug(f"Erro ao consultar DESCRIBE DATABASE EXTENDED {schema}: {e}")
        return None
    return None


def database_exists(cur, schema: str) -> bool:
    """Verifica se o schema existe no catálogo via SHOW DATABASES."""
    cur.execute("SHOW DATABASES")
    dbs = [str(r[0]).strip().lower() for r in cur.fetchall()]
    return schema.strip().lower() in dbs


def bootstrap_database(cur, schema: str, expected_location: str) -> Tuple[bool, str]:
    """
    Verifica e cria de forma segura o schema com o location esperado.
    Retorna (sucesso: bool, mensagem: str).
    """
    schema_clean = schema.strip()
    expected_norm = normalize_location(expected_location)

    if not expected_norm:
        return False, "Expected location não pode ser vazio."

    exists = database_exists(cur, schema_clean)

    if not exists:
        logger.info(f"Schema '{schema_clean}' não existe. Criando com LOCATION '{expected_location}'...")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {schema_clean} LOCATION '{expected_location}'")

        # Validação pós-criação
        actual_loc = get_database_location(cur, schema_clean)
        if not actual_loc:
            return False, f"Schema '{schema_clean}' foi criado mas não foi possível verificar seu location."

        actual_norm = normalize_location(actual_loc)
        if actual_norm != expected_norm:
            return False, (
                f"FALHA CRÍTICA: Schema '{schema_clean}' foi criado com location inesperado: "
                f"Esperado='{expected_location}', Atual='{actual_loc}'."
            )

        return True, f"Schema '{schema_clean}' criado com sucesso no location '{actual_loc}'."

    # Schema já existe
    actual_loc = get_database_location(cur, schema_clean)
    if not actual_loc:
        return False, f"Schema '{schema_clean}' já existe no catálogo, mas não foi possível ler seu location."

    actual_norm = normalize_location(actual_loc)
    if actual_norm == expected_norm:
        return True, (
            f"Schema '{schema_clean}' já existe e está conforme com o location esperado "
            f"'{actual_loc}' (NOOP idempotente)."
        )

    # Caso C: Schema existe mas location difere
    return False, (
        f"CONFLITO CRÍTICO DE LOCATION: Schema '{schema_clean}' já existe mas aponta para "
        f"'{actual_loc}', que difere do esperado '{expected_location}'. "
        f"Operação abortada para prevenir contaminação ou sobrescrita acidental de dados!"
    )


def get_connection(host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
    return hive.Connection(host=host, port=port, username=user)


def main():
    parser = argparse.ArgumentParser(description="Bootstrap reproduzível e seguro de schema no Lakehouse (R3-B.1).")
    parser.add_argument("--schema", default="silver", help="Nome do schema/database (default: silver)")
    parser.add_argument("--location", default="s3a://silver/warehouse", help="URI do location esperado no S3/MinIO")
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Spark Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Spark Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuário do Spark Thrift Server")

    args = parser.parse_args()

    logger.info(f"Iniciando verificação de bootstrap para o schema '{args.schema}'...")
    logger.info(f"Location esperado: '{args.location}'")
    logger.info(f"Conectando a {args.host}:{args.port} como '{args.user}'...")

    try:
        conn = get_connection(host=args.host, port=args.port, user=args.user)
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Falha ao conectar no Spark Thrift Server ({args.host}:{args.port}): {e}")
        sys.exit(1)

    try:
        success, message = bootstrap_database(cur, args.schema, args.location)
        if success:
            logger.info(f"[SUCESSO] {message}")
            sys.exit(0)
        else:
            logger.error(f"[ERRO] {message}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado durante bootstrap do schema '{args.schema}': {e}")
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
