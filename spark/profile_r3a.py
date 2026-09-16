"""
Módulo de profiling reproduzível e diagnóstico da camada Bronze (R3-A).
Executa análises estruturais, qualidade de colunas, unicidade, cardinalidade,
integridade referencial e simulação de riscos de joins e duplicidade financeira.
"""
import os
import sys
import json
import time
import argparse
import logging
from typing import Dict, Any, List, Optional
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("profile_r3a")

TABLES = [
    "siconv_programa",
    "siconv_programa_proposta",
    "siconv_proposta",
    "siconv_convenio"
]

METADATA_COLS = {
    "__ingested_at_utc",
    "__ingestion_run_id",
    "__source_file",
    "__source_sha256"
}

def get_connection(host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
    return hive.Connection(host=host, port=port, username=user)

def get_snapshot_info(conn) -> Dict[str, Any]:
    logger.info("Coletando informacoes do snapshot ativo...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ingestion_run_id, status, source_data_carga_raw_final,
               source_data_carga_final, start_time_utc, end_time_utc
        FROM bronze.ingestion_runs
        ORDER BY start_time_utc DESC
        LIMIT 5
    """)
    runs = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM bronze.ingestion_runs")
    total_runs = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM bronze.ingestion_manifest")
    total_manifest = cursor.fetchone()[0]

    cursor.execute("""
        SELECT dataset_id, delta_output_rows, delta_version, status, ingestion_run_id
        FROM bronze.ingestion_manifest
        ORDER BY ingestion_run_id DESC, dataset_id
    """)
    manifest_rows = cursor.fetchall()
    cursor.close()

    return {
        "total_runs": total_runs,
        "total_manifest": total_manifest,
        "runs": runs,
        "manifest": manifest_rows
    }

def get_table_schema(conn, table_name: str) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE bronze.{table_name}")
    rows = cursor.fetchall()
    cursor.close()
    cols = []
    for r in rows:
        name = r[0].strip()
        dtype = r[1].strip() if r[1] else "string"
        if not name.startswith("#") and name:
            cols.append({"name": name, "type": dtype, "is_metadata": name in METADATA_COLS})
    return cols

def profile_table_columns(conn, table_name: str, cols: List[Dict[str, Any]], sample_limit: int = 10) -> Dict[str, Any]:
    logger.info(f"Profiling colunas de {table_name}...")
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM bronze.{table_name}")
    total_rows = cursor.fetchone()[0]

    col_profiles = {}
    official_cols = [c["name"] for c in cols if not c["is_metadata"]]
    batch_size = 5

    for i in range(0, len(official_cols), batch_size):
        batch = official_cols[i:i + batch_size]
        select_parts = []
        for col in batch:
            select_parts.extend([
                f"COUNT(CASE WHEN {col} IS NULL THEN 1 END) as {col}_nulls",
                f"COUNT(CASE WHEN {col} IS NOT NULL AND TRIM({col}) = '' THEN 1 END) as {col}_empty",
                f"COUNT(CASE WHEN {col} IS NOT NULL AND {col} != '' AND TRIM({col}) = '' THEN 1 END) as {col}_blank_spaces",
                f"COUNT(DISTINCT {col}) as {col}_dist",
                f"MIN(LENGTH({col})) as {col}_min_len",
                f"MAX(LENGTH({col})) as {col}_max_len"
            ])
        query = f"SELECT {', '.join(select_parts)} FROM bronze.{table_name}"
        cursor.execute(query)
        res = cursor.fetchone()

        idx = 0
        for col in batch:
            null_count = res[idx]
            empty_count = res[idx + 1]
            blank_spaces = res[idx + 2]
            distinct_count = res[idx + 3]
            min_len = res[idx + 4]
            max_len = res[idx + 5]
            idx += 6

            null_pct = (null_count / total_rows * 100.0) if total_rows > 0 else 0.0
            distinct_pct = (distinct_count / total_rows * 100.0) if total_rows > 0 else 0.0

            col_profiles[col] = {
                "name": col,
                "total_rows": total_rows,
                "null_count": null_count,
                "null_pct": round(null_pct, 4),
                "empty_string_count": empty_count,
                "blank_spaces_count": blank_spaces,
                "distinct_count": distinct_count,
                "distinct_pct": round(distinct_pct, 4),
                "min_length": min_len,
                "max_length": max_len,
                "top_values": []
            }

    for col, prof in col_profiles.items():
        if prof["distinct_count"] <= 35:
            cursor.execute(f"""
                SELECT COALESCE({col}, '<NULL>'), COUNT(*) as cnt
                FROM bronze.{table_name}
                GROUP BY {col}
                ORDER BY cnt DESC
                LIMIT {sample_limit}
            """)
            prof["top_values"] = [{"value": str(r[0]), "count": r[1]} for r in cursor.fetchall()]

    cursor.close()
    return {
        "table_name": table_name,
        "total_rows": total_rows,
        "columns": col_profiles
    }

def run_profiling(host: str, port: int, user: str, output_file: Optional[str] = None):
    conn = get_connection(host, port, user)
    logger.info("Conexao estabelecida com sucesso com o Spark Thrift Server.")

    results = {}
    t0 = time.time()

    results["snapshot"] = get_snapshot_info(conn)

    table_profiles = {}
    schemas = {}
    for tbl in TABLES:
        schema = get_table_schema(conn, tbl)
        schemas[tbl] = schema
        prof = profile_table_columns(conn, tbl, schema)
        table_profiles[tbl] = prof

    results["schemas"] = schemas
    results["profiles"] = table_profiles

    logger.info("Executando analises relacionais e simulacao de joins...")
    cursor = conn.cursor()

    # 1. Convenio -> Proposta
    cursor.execute("""
        SELECT
            COUNT(DISTINCT c.ID_PROPOSTA) as conv_props_matched,
            COUNT(DISTINCT CASE WHEN p.ID_PROPOSTA IS NULL THEN c.ID_PROPOSTA END) as conv_props_unmatched
        FROM bronze.siconv_convenio c
        LEFT JOIN bronze.siconv_proposta p ON c.ID_PROPOSTA = p.ID_PROPOSTA
    """)
    conv_to_prop = cursor.fetchone()

    # 2. Programa_proposta -> Proposta
    cursor.execute("""
        SELECT
            COUNT(DISTINCT pp.ID_PROPOSTA) as pp_matched,
            COUNT(DISTINCT CASE WHEN p.ID_PROPOSTA IS NULL THEN pp.ID_PROPOSTA END) as pp_unmatched
        FROM bronze.siconv_programa_proposta pp
        LEFT JOIN bronze.siconv_proposta p ON pp.ID_PROPOSTA = p.ID_PROPOSTA
    """)
    pp_to_prop = cursor.fetchone()

    # 3. Programa_proposta -> Programa
    cursor.execute("""
        SELECT
            COUNT(DISTINCT pp.ID_PROGRAMA) as pp_prog_total,
            COUNT(DISTINCT CASE WHEN p.ID_PROGRAMA IS NOT NULL THEN pp.ID_PROGRAMA END) as pp_matched,
            COUNT(DISTINCT CASE WHEN p.ID_PROGRAMA IS NULL THEN pp.ID_PROGRAMA END) as pp_unmatched
        FROM bronze.siconv_programa_proposta pp
        LEFT JOIN (SELECT DISTINCT ID_PROGRAMA FROM bronze.siconv_programa) p ON pp.ID_PROGRAMA = p.ID_PROGRAMA
    """)
    pp_to_prog = cursor.fetchone()

    # 4. Proposta -> Convenio
    cursor.execute("""
        SELECT
            COUNT(DISTINCT p.ID_PROPOSTA) as total_props,
            COUNT(DISTINCT c.ID_PROPOSTA) as props_with_conv,
            COUNT(DISTINCT CASE WHEN c.ID_PROPOSTA IS NULL THEN p.ID_PROPOSTA END) as props_without_conv
        FROM bronze.siconv_proposta p
        LEFT JOIN bronze.siconv_convenio c ON p.ID_PROPOSTA = c.ID_PROPOSTA
    """)
    prop_to_conv = cursor.fetchone()

    # 5. Join multiplication simulation
    cursor.execute("SELECT COUNT(*) FROM bronze.siconv_convenio")
    cnt_conv = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM bronze.siconv_convenio c
        INNER JOIN bronze.siconv_proposta p ON c.ID_PROPOSTA = p.ID_PROPOSTA
    """)
    cnt_conv_prop = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM bronze.siconv_convenio c
        INNER JOIN bronze.siconv_proposta p ON c.ID_PROPOSTA = p.ID_PROPOSTA
        INNER JOIN bronze.siconv_programa_proposta pp ON p.ID_PROPOSTA = pp.ID_PROPOSTA
    """)
    cnt_conv_prop_pp = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM bronze.siconv_convenio c
        INNER JOIN bronze.siconv_proposta p ON c.ID_PROPOSTA = p.ID_PROPOSTA
        INNER JOIN bronze.siconv_programa_proposta pp ON p.ID_PROPOSTA = pp.ID_PROPOSTA
        INNER JOIN bronze.siconv_programa prog ON pp.ID_PROGRAMA = prog.ID_PROGRAMA
    """)
    cnt_conv_prop_pp_prog = cursor.fetchone()[0]

    # 6. Financial sum inflation simulation
    cursor.execute("""
        SELECT SUM(CAST(REPLACE(VL_GLOBAL_CONV, ',', '.') AS DOUBLE))
        FROM bronze.siconv_convenio
    """)
    sum_conv_base = cursor.fetchone()[0]

    cursor.execute("""
        SELECT SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DOUBLE))
        FROM bronze.siconv_convenio c
        INNER JOIN bronze.siconv_programa_proposta pp ON c.ID_PROPOSTA = pp.ID_PROPOSTA
    """)
    sum_conv_pp = cursor.fetchone()[0]

    cursor.execute("""
        SELECT SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DOUBLE))
        FROM bronze.siconv_convenio c
        INNER JOIN bronze.siconv_programa_proposta pp ON c.ID_PROPOSTA = pp.ID_PROPOSTA
        INNER JOIN bronze.siconv_programa prog ON pp.ID_PROGRAMA = prog.ID_PROGRAMA
    """)
    sum_conv_prog = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    results["relations"] = {
        "convenio_to_proposta": {"matched": conv_to_prop[0], "unmatched": conv_to_prop[1]},
        "programa_proposta_to_proposta": {"matched": pp_to_prop[0], "unmatched": pp_to_prop[1]},
        "programa_proposta_to_programa": {"total": pp_to_prog[0], "matched": pp_to_prog[1], "unmatched": pp_to_prog[2]},
        "proposta_to_convenio": {"total": prop_to_conv[0], "with_conv": prop_to_conv[1], "without_conv": prop_to_conv[2]},
        "join_multiplication": {
            "convenio_base": cnt_conv,
            "convenio_join_proposta": cnt_conv_prop,
            "convenio_join_proposta_join_pp": cnt_conv_prop_pp,
            "convenio_join_proposta_join_pp_join_programa": cnt_conv_prop_pp_prog
        },
        "financial_inflation": {
            "base_sum": sum_conv_base,
            "sum_after_pp": sum_conv_pp,
            "sum_after_programa": sum_conv_prog
        }
    }

    duration = time.time() - t0
    results["profiling_duration_seconds"] = round(duration, 2)
    logger.info(f"Profiling concluido em {duration:.2f}s")

    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Resultados persistidos em {output_file}")

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profiling R3-A Camada Bronze Transferegov")
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuario Thrift Server")
    parser.add_argument("--output", default=None, help="Caminho do arquivo JSON de saida")
    args = parser.parse_args()

    res = run_profiling(args.host, args.port, args.user, args.output)
    print(f"Profiling finalizado com sucesso. Tabelas analisadas: {list(res.get('profiles', {}).keys())}")
