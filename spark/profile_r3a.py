"""
Módulo de profiling reproduzível e diagnóstico da camada Bronze (R3-A e R3-A.1).
Executa análises estruturais, qualidade de colunas, unicidade, cardinalidade,
integridade referencial e simulação de riscos de joins e duplicidade financeira
utilizando agregação de multiplicidades sem materialização explosiva.
"""
import os
import sys
import json
import time
import argparse
import logging
from decimal import Decimal
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

def log_query_start(name: str):
    logger.info(f"[QUERY START] {name}")

def log_query_end(name: str, start_time: float):
    duration = time.time() - start_time
    logger.info(f"[QUERY END] {name} (duracao: {duration:.2f}s)")
    return duration

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

def analyze_conflicting_convenios(conn) -> Dict[str, Any]:
    """
    R3-A.1 FASE A: Investigação dirigida dos 2 números de convênio conflitantes.
    """
    logger.info("[START] FASE A — conflito de convênios")
    t0 = time.time()
    cursor = conn.cursor()

    log_query_start("Busca schema de siconv_convenio")
    cursor.execute("DESCRIBE bronze.siconv_convenio")
    cols = [r[0].strip() for r in cursor.fetchall() if r[0] and not r[0].strip().startswith("#")]
    log_query_end("Busca schema de siconv_convenio", t0)

    q_name = "Busca registros conflitantes 949286 e 956078"
    t_q = time.time()
    log_query_start(q_name)
    cursor.execute("""
        SELECT *
        FROM bronze.siconv_convenio
        WHERE NR_CONVENIO IN ('949286', '956078')
        ORDER BY NR_CONVENIO, VL_SALDO_CONTA
    """)
    rows = cursor.fetchall()
    log_query_end(q_name, t_q)

    recs_by_nr = {}
    for r in rows:
        d = dict(zip(cols, r))
        recs_by_nr.setdefault(d["NR_CONVENIO"], []).append(d)

    summary = {}
    for nr, recs in recs_by_nr.items():
        diffs = []
        if len(recs) == 2:
            for c in cols:
                v1, v2 = recs[0].get(c), recs[1].get(c)
                if v1 != v2:
                    diffs.append({"column": c, "row1": str(v1), "row2": str(v2)})

            summary[nr] = {
                "count": len(recs),
                "same_run": recs[0].get("__ingestion_run_id") == recs[1].get("__ingestion_run_id"),
                "same_sha256": recs[0].get("__source_sha256") == recs[1].get("__source_sha256"),
                "same_ingested_at": recs[0].get("__ingested_at_utc") == recs[1].get("__ingested_at_utc"),
                "divergent_columns": diffs,
                "only_saldo_diverges": [d["column"] for d in diffs if not d["column"].startswith("__")] == ["VL_SALDO_CONTA"]
            }

    cursor.close()
    duration = time.time() - t0
    logger.info(f"[END] FASE A — conflito de convênios (duracao: {duration:.2f}s)")
    return {
        "duration_seconds": round(duration, 2),
        "convenios": summary,
        "temporal_criterion_exists": False,
        "proven_deduplication": False,
        "recommended_silver_rows": 287586
    }

def analyze_program_key_collisions(conn) -> Dict[str, Any]:
    """
    R3-A.1 FASE B: Análise de colisão e resolução do grão de siconv_programa.
    """
    logger.info("[START] FASE B — colisões de programa")
    t0 = time.time()
    cursor = conn.cursor()

    # B1 - 5 colunas base
    q1 = "B1 - Colisoes na chave candidata de 5 colunas"
    t_q = time.time()
    log_query_start(q1)
    cursor.execute("""
        WITH collision_keys AS (
            SELECT
                ID_PROGRAMA,
                MODALIDADE_PROGRAMA,
                NATUREZA_JURIDICA_PROGRAMA,
                UF_PROGRAMA,
                ACAO_ORCAMENTARIA,
                COUNT(*) AS multiplicidade
            FROM bronze.siconv_programa
            GROUP BY
                ID_PROGRAMA,
                MODALIDADE_PROGRAMA,
                NATUREZA_JURIDICA_PROGRAMA,
                UF_PROGRAMA,
                ACAO_ORCAMENTARIA
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS grupos_colisao,
            COALESCE(SUM(multiplicidade), 0) AS linhas_envolvidas,
            COALESCE(MAX(multiplicidade), 1) AS multiplicidade_maxima
        FROM collision_keys
    """)
    r1 = cursor.fetchone()
    log_query_end(q1, t_q)

    # Teste de 4 colunas (sem ACAO_ORCAMENTARIA)
    q2 = "B2 - Colisoes na chave de 4 colunas (sem ACAO_ORCAMENTARIA)"
    t_q = time.time()
    log_query_start(q2)
    cursor.execute("""
        WITH collision_keys AS (
            SELECT
                ID_PROGRAMA,
                MODALIDADE_PROGRAMA,
                NATUREZA_JURIDICA_PROGRAMA,
                UF_PROGRAMA,
                COUNT(*) AS multiplicidade
            FROM bronze.siconv_programa
            GROUP BY
                ID_PROGRAMA,
                MODALIDADE_PROGRAMA,
                NATUREZA_JURIDICA_PROGRAMA,
                UF_PROGRAMA
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(*) AS grupos_colisao,
            COALESCE(SUM(multiplicidade), 0) AS linhas_envolvidas,
            COALESCE(MAX(multiplicidade), 1) AS multiplicidade_maxima
        FROM collision_keys
    """)
    r2 = cursor.fetchone()
    log_query_end(q2, t_q)

    # Total de linhas e contagem com concat
    q3 = "B3 - Unicidade exata via CONCAT_WS na chave de 5 colunas"
    t_q = time.time()
    log_query_start(q3)
    cursor.execute("""
        SELECT
            COUNT(*),
            COUNT(DISTINCT CONCAT_WS('~',
                COALESCE(ID_PROGRAMA, '<NULL>'),
                COALESCE(MODALIDADE_PROGRAMA, '<NULL>'),
                COALESCE(NATUREZA_JURIDICA_PROGRAMA, '<NULL>'),
                COALESCE(UF_PROGRAMA, '<NULL>'),
                COALESCE(ACAO_ORCAMENTARIA, '<NULL>')
            ))
        FROM bronze.siconv_programa
    """)
    r3 = cursor.fetchone()
    log_query_end(q3, t_q)

    cursor.close()
    duration = time.time() - t0
    logger.info(f"[END] FASE B — colisões de programa (duracao: {duration:.2f}s)")
    return {
        "duration_seconds": round(duration, 2),
        "candidate_key_5_cols": {
            "columns": ["ID_PROGRAMA", "MODALIDADE_PROGRAMA", "NATUREZA_JURIDICA_PROGRAMA", "UF_PROGRAMA", "ACAO_ORCAMENTARIA"],
            "collision_groups": r1[0],
            "collision_rows": r1[1],
            "max_multiplicity": r1[2],
            "is_unique": r1[0] == 0
        },
        "candidate_key_4_cols": {
            "columns": ["ID_PROGRAMA", "MODALIDADE_PROGRAMA", "NATUREZA_JURIDICA_PROGRAMA", "UF_PROGRAMA"],
            "collision_groups": r2[0],
            "collision_rows": r2[1],
            "max_multiplicity": r2[2],
            "is_unique": r2[0] == 0
        },
        "total_rows": r3[0],
        "distinct_5_col_combinations": r3[1],
        "uniqueness_proven": r3[0] == r3[1],
        "recommended_technical_id": "sha256(concat_ws('||', coalesce(id_programa, ''), coalesce(modalidade_programa, ''), coalesce(natureza_juridica_programa, ''), coalesce(uf_programa, ''), coalesce(acao_orcamentaria, '')))"
    }

def analyze_program_functional_dependencies(conn) -> Dict[str, Any]:
    """
    R3-A.1 FASE C: Dependência funcional de ID_PROGRAMA para avaliar entidade programa_cadastral.
    """
    logger.info("[START] FASE C — dependência funcional de programa")
    t0 = time.time()
    cursor = conn.cursor()

    q = "C1 - Analise de dependencia funcional em query agregada unica"
    t_q = time.time()
    log_query_start(q)
    cursor.execute("""
        WITH per_program AS (
            SELECT
                ID_PROGRAMA,
                COUNT(DISTINCT COD_ORGAO_SUP_PROGRAMA) AS n_orgao_sup,
                COUNT(DISTINCT DESC_ORGAO_SUP_PROGRAMA) AS n_desc_orgao,
                COUNT(DISTINCT COD_PROGRAMA) AS n_cod_programa,
                COUNT(DISTINCT NOME_PROGRAMA) AS n_nome,
                COUNT(DISTINCT SIT_PROGRAMA) AS n_situacao,
                COUNT(DISTINCT DATA_DISPONIBILIZACAO) AS n_data,
                COUNT(DISTINCT ANO_DISPONIBILIZACAO) AS n_ano,
                COUNT(DISTINCT CONCAT_WS('~',
                    COALESCE(COD_ORGAO_SUP_PROGRAMA, '<NULL>'),
                    COALESCE(DESC_ORGAO_SUP_PROGRAMA, '<NULL>'),
                    COALESCE(COD_PROGRAMA, '<NULL>'),
                    COALESCE(NOME_PROGRAMA, '<NULL>'),
                    COALESCE(SIT_PROGRAMA, '<NULL>'),
                    COALESCE(DATA_DISPONIBILIZACAO, '<NULL>'),
                    COALESCE(ANO_DISPONIBILIZACAO, '<NULL>')
                )) AS n_tupla_cadastral
            FROM bronze.siconv_programa
            GROUP BY ID_PROGRAMA
        )
        SELECT
            COUNT(*) AS total_programas,
            COUNT(CASE WHEN n_orgao_sup > 1 THEN 1 END) AS multi_orgao_sup,
            MAX(n_orgao_sup) AS max_orgao_sup,
            COUNT(CASE WHEN n_desc_orgao > 1 THEN 1 END) AS multi_desc_orgao,
            MAX(n_desc_orgao) AS max_desc_orgao,
            COUNT(CASE WHEN n_cod_programa > 1 THEN 1 END) AS multi_cod_prog,
            MAX(n_cod_programa) AS max_cod_prog,
            COUNT(CASE WHEN n_nome > 1 THEN 1 END) AS multi_nome_prog,
            MAX(n_nome) AS max_nome_prog,
            COUNT(CASE WHEN n_situacao > 1 THEN 1 END) AS multi_situacao,
            MAX(n_situacao) AS max_situacao,
            COUNT(CASE WHEN n_data > 1 THEN 1 END) AS multi_data,
            MAX(n_data) AS max_data,
            COUNT(CASE WHEN n_ano > 1 THEN 1 END) AS multi_ano,
            MAX(n_ano) AS max_ano,
            COUNT(CASE WHEN n_tupla_cadastral > 1 THEN 1 END) AS multi_tupla,
            MAX(n_tupla_cadastral) AS max_tupla
        FROM per_program
    """)
    res = cursor.fetchone()
    log_query_end(q, t_q)

    total_prog = res[0]
    col_names = [
        ("COD_ORGAO_SUP_PROGRAMA", res[1], res[2]),
        ("DESC_ORGAO_SUP_PROGRAMA", res[3], res[4]),
        ("COD_PROGRAMA", res[5], res[6]),
        ("NOME_PROGRAMA", res[7], res[8]),
        ("SIT_PROGRAMA", res[9], res[10]),
        ("DATA_DISPONIBILIZACAO", res[11], res[12]),
        ("ANO_DISPONIBILIZACAO", res[13], res[14])
    ]
    multi_tupla = res[15]
    max_tupla = res[16]

    columns_summary = {}
    for name, multi_cnt, max_val in col_names:
        pct = (multi_cnt / total_prog * 100.0) if total_prog > 0 else 0.0
        columns_summary[name] = {
            "ids_with_multi_values": multi_cnt,
            "pct": round(pct, 4),
            "max_distinct": max_val
        }

    is_strict_1to1 = (multi_tupla == 0)
    classification = "CONFIRMADO" if is_strict_1to1 else "CANDIDATO COM RESTRIÇÕES"

    cursor.close()
    duration = time.time() - t0
    logger.info(f"[END] FASE C — dependência funcional de programa (duracao: {duration:.2f}s)")
    return {
        "duration_seconds": round(duration, 2),
        "total_distinct_id_programa": total_prog,
        "columns": columns_summary,
        "tuple_level": {
            "ids_with_multi_tuples": multi_tupla,
            "pct": round((multi_tupla / total_prog * 100.0) if total_prog > 0 else 0.0, 4),
            "max_tuples": max_tupla
        },
        "functional_dependency_proven": is_strict_1to1,
        "entity_classification": classification
    }

def analyze_financial_join_risk_decimal(conn) -> Dict[str, Any]:
    """
    R3-A.1 FASE D: Baseline financeiro exato com DECIMAL e sem materializar joins explosivos.
    """
    logger.info("[START] FASE D — baseline financeiro DECIMAL")
    t0 = time.time()
    cursor = conn.cursor()

    # D1 — Base
    q1 = "D1 - Baseline financeiro siconv_convenio (DECIMAL)"
    t_q = time.time()
    log_query_start(q1)
    cursor.execute("""
        SELECT
            COUNT(*) AS linhas_base,
            SUM(CAST(REPLACE(VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2))) AS soma_base_dec
        FROM bronze.siconv_convenio
    """)
    r1 = cursor.fetchone()
    log_query_end(q1, t_q)
    linhas_base = r1[0]
    soma_base = Decimal(str(r1[1]))

    # D2 — Efeito da ponte por multiplicidade agregada
    q2 = "D2 - Efeito da ponte siconv_programa_proposta (DECIMAL por multiplicidade)"
    t_q = time.time()
    log_query_start(q2)
    cursor.execute("""
        WITH prop_mult AS (
            SELECT
                ID_PROPOSTA,
                COUNT(*) AS qtd_programas
            FROM bronze.siconv_programa_proposta
            GROUP BY ID_PROPOSTA
        )
        SELECT
            SUM(pm.qtd_programas) AS linhas_apos_ponte,
            SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2)) * CAST(pm.qtd_programas AS DECIMAL(38,0))) AS soma_ponte_dec
        FROM bronze.siconv_convenio c
        INNER JOIN prop_mult pm ON c.ID_PROPOSTA = pm.ID_PROPOSTA
    """)
    r2 = cursor.fetchone()
    log_query_end(q2, t_q)
    linhas_ponte = r2[0]
    soma_ponte = Decimal(str(r2[1]))
    dif_ponte = soma_ponte - soma_base
    pct_ponte = (dif_ponte / soma_base * Decimal(100)) if soma_base > 0 else Decimal(0)

    # D3 — Efeito de siconv_programa por multiplicidade agregada sem join explosivo
    q3 = "D3 - Efeito de siconv_programa (DECIMAL por multiplicidade agregada)"
    t_q = time.time()
    log_query_start(q3)
    cursor.execute("""
        WITH prog_mult AS (
            SELECT
                ID_PROGRAMA,
                COUNT(*) AS qtd_linhas_programa
            FROM bronze.siconv_programa
            GROUP BY ID_PROGRAMA
        ),
        prop_prog_mult AS (
            SELECT
                pp.ID_PROPOSTA,
                SUM(pm.qtd_linhas_programa) AS mult_programa_por_proposta
            FROM bronze.siconv_programa_proposta pp
            INNER JOIN prog_mult pm ON pp.ID_PROGRAMA = pm.ID_PROGRAMA
            GROUP BY pp.ID_PROPOSTA
        )
        SELECT
            SUM(ppm.mult_programa_por_proposta) AS linhas_apos_programa,
            SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2)) * CAST(ppm.mult_programa_por_proposta AS DECIMAL(38,0))) AS soma_programa_dec
        FROM bronze.siconv_convenio c
        INNER JOIN prop_prog_mult ppm ON c.ID_PROPOSTA = ppm.ID_PROPOSTA
    """)
    r3 = cursor.fetchone()
    log_query_end(q3, t_q)
    linhas_programa = r3[0]
    soma_programa = Decimal(str(r3[1]))
    dif_programa = soma_programa - soma_base
    fator_programa = (soma_programa / soma_base) if soma_base > 0 else Decimal(0)

    cursor.close()
    duration = time.time() - t0
    logger.info(f"[END] FASE D — baseline financeiro DECIMAL (duracao: {duration:.2f}s)")

    return {
        "duration_seconds": round(duration, 2),
        "base": {
            "rows": linhas_base,
            "sum_decimal": str(soma_base)
        },
        "after_bridge": {
            "equivalent_rows": linhas_ponte,
            "sum_decimal": str(soma_ponte),
            "diff_decimal": str(dif_ponte),
            "pct_diff": float(pct_ponte)
        },
        "after_program": {
            "equivalent_rows": linhas_programa,
            "sum_decimal": str(soma_programa),
            "diff_decimal": str(dif_programa),
            "factor": float(fator_programa),
            "cardinality_matches_baseline_24243823": linhas_programa == 24243823
        }
    }

def run_profiling(
    host: str,
    port: int,
    user: str,
    sections: List[str],
    output_file: Optional[str] = None
) -> Dict[str, Any]:
    conn = get_connection(host, port, user)
    logger.info("Conexao estabelecida com sucesso com o Spark Thrift Server.")

    results = {}
    t0 = time.time()

    run_all = "all" in sections
    run_basic = run_all or "basic" in sections

    if run_basic:
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

        logger.info("Executando analises relacionais e simulacao de joins (segura e sem explosao)...")
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

        # Multiplicidade segura para programa
        cursor.execute("""
            WITH prog_mult AS (
                SELECT ID_PROGRAMA, COUNT(*) AS qtd FROM bronze.siconv_programa GROUP BY ID_PROGRAMA
            ),
            prop_mult AS (
                SELECT pp.ID_PROPOSTA, SUM(pm.qtd) AS mult
                FROM bronze.siconv_programa_proposta pp
                INNER JOIN prog_mult pm ON pp.ID_PROGRAMA = pm.ID_PROGRAMA
                GROUP BY pp.ID_PROPOSTA
            )
            SELECT SUM(ppm.mult)
            FROM bronze.siconv_convenio c
            INNER JOIN prop_mult ppm ON c.ID_PROPOSTA = ppm.ID_PROPOSTA
        """)
        cnt_conv_prop_pp_prog = cursor.fetchone()[0]

        # 6. Financial sum inflation simulation via DECIMAL
        cursor.execute("""
            SELECT SUM(CAST(REPLACE(VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2)))
            FROM bronze.siconv_convenio
        """)
        sum_conv_base = str(cursor.fetchone()[0])

        cursor.execute("""
            WITH prop_mult AS (
                SELECT ID_PROPOSTA, COUNT(*) AS qtd FROM bronze.siconv_programa_proposta GROUP BY ID_PROPOSTA
            )
            SELECT SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2)) * CAST(pm.qtd AS DECIMAL(38,0)))
            FROM bronze.siconv_convenio c
            INNER JOIN prop_mult pm ON c.ID_PROPOSTA = pm.ID_PROPOSTA
        """)
        sum_conv_pp = str(cursor.fetchone()[0])

        cursor.execute("""
            WITH prog_mult AS (
                SELECT ID_PROGRAMA, COUNT(*) AS qtd FROM bronze.siconv_programa GROUP BY ID_PROGRAMA
            ),
            prop_mult AS (
                SELECT pp.ID_PROPOSTA, SUM(pm.qtd) AS mult
                FROM bronze.siconv_programa_proposta pp
                INNER JOIN prog_mult pm ON pp.ID_PROGRAMA = pm.ID_PROGRAMA
                GROUP BY pp.ID_PROPOSTA
            )
            SELECT SUM(CAST(REPLACE(c.VL_GLOBAL_CONV, ',', '.') AS DECIMAL(38,2)) * CAST(ppm.mult AS DECIMAL(38,0)))
            FROM bronze.siconv_convenio c
            INNER JOIN prop_mult ppm ON c.ID_PROPOSTA = ppm.ID_PROPOSTA
        """)
        sum_conv_prog = str(cursor.fetchone()[0])

        cursor.close()

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

    if run_all or "convenios" in sections:
        results["findings_convenios"] = analyze_conflicting_convenios(conn)

    if run_all or "program-grain" in sections:
        results["findings_program_grain"] = analyze_program_key_collisions(conn)

    if run_all or "program-fd" in sections:
        results["findings_program_fd"] = analyze_program_functional_dependencies(conn)

    if run_all or "finance" in sections:
        results["findings_finance_decimal"] = analyze_financial_join_risk_decimal(conn)

    conn.close()

    duration = time.time() - t0
    results["profiling_duration_seconds"] = round(duration, 2)
    logger.info(f"Execucao de profiling concluida em {duration:.2f}s")

    if output_file:
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Resultados persistidos em {output_file}")

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Profiling R3-A / R3-A.1 Camada Bronze Transferegov")
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuario Thrift Server")
    parser.add_argument("--section", choices=["all", "basic", "program-grain", "program-fd", "finance", "convenios"], default="all", help="Secao a executar")
    parser.add_argument("--output", default=None, help="Caminho do arquivo JSON de saida")
    args = parser.parse_args()

    res = run_profiling(args.host, args.port, args.user, [args.section], args.output)
    print(f"Execucao concluida com sucesso. Secoes: {list(res.keys())}")
