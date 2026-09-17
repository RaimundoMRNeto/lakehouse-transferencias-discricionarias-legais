"""
Módulo de profiling dirigido e descoberta analítica para o contrato da camada Gold (R4-A).
Executa diagnósticos empíricos sobre a camada Silver (siconv_proposta, siconv_programa_cadastral,
siconv_programa_elegibilidade, siconv_programa_proposta, siconv_convenio) sem materialização,
sem escritas e sem alteração do catálogo.

Suporta execução modular via CLI:
  --section snapshot
  --section proponente
  --section municipio
  --section orgao
  --section convenio
  --section programa-nn
  --section datas
  --section metricas
  --section all
"""
import sys
import os
import json
import time
import argparse
import logging
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("profile_r4a")

SILVER_TABLES = [
    "siconv_proposta",
    "siconv_programa_cadastral",
    "siconv_programa_elegibilidade",
    "siconv_programa_proposta",
    "siconv_convenio"
]

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        return super().default(obj)

def get_connection(host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
    conn = hive.Connection(host=host, port=port, username=user)
    cursor = conn.cursor()
    cursor.execute("SET spark.sql.autoBroadcastJoinThreshold = -1")
    cursor.close()
    return conn

def log_query_start(name: str):
    logger.info(f"[QUERY START] {name}")

def log_query_end(name: str, start_time: float) -> float:
    duration = time.time() - start_time
    logger.info(f"[QUERY END] {name} (duracao: {duration:.2f}s)")
    return duration

# ==============================================================================
# SEÇÃO 1: SNAPSHOT
# ==============================================================================
def run_snapshot_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=snapshot")
    t0 = time.time()
    cursor = conn.cursor()

    exec_time_utc = datetime.now(timezone.utc).isoformat()

    # 1. Catálogo Gold (Read-only)
    log_query_start("Verifica bancos no catalogo Hive/Spark")
    t_q = time.time()
    cursor.execute("SHOW DATABASES")
    dbs = [r[0] for r in cursor.fetchall()]
    log_query_end("Verifica bancos no catalogo Hive/Spark", t_q)

    gold_exists = "gold" in dbs
    gold_location = None
    if gold_exists:
        log_query_start("Descreve banco gold")
        t_q = time.time()
        cursor.execute("DESCRIBE DATABASE EXTENDED gold")
        for row in cursor.fetchall():
            if len(row) >= 2 and row[0].strip().lower() == "location":
                gold_location = row[1].strip()
        log_query_end("Descreve banco gold", t_q)

    # 2. Metadados e contagens das 5 tabelas Silver
    tables_meta = {}
    for table_name in SILVER_TABLES:
        q_name = f"Coleta metadados de snapshot silver.{table_name}"
        log_query_start(q_name)
        t_q = time.time()
        cursor.execute(f"""
            SELECT
                COUNT(*) as cnt,
                COUNT(DISTINCT __ingestion_run_id) as n_runs,
                MIN(__ingestion_run_id) as min_run,
                MAX(__ingestion_run_id) as max_run,
                COUNT(DISTINCT __source_sha256) as n_shas,
                MIN(__source_sha256) as min_sha,
                MAX(__source_sha256) as max_sha,
                MIN(__ingested_at_utc) as min_ingested,
                MAX(__ingested_at_utc) as max_ingested
            FROM silver.{table_name}
        """)
        row = cursor.fetchone()
        log_query_end(q_name, t_q)

        tables_meta[table_name] = {
            "row_count": row[0],
            "distinct_runs": row[1],
            "min_run": row[2],
            "max_run": row[3],
            "distinct_sha256": row[4],
            "min_sha256": row[5],
            "max_sha256": row[6],
            "min_ingested_at_utc": str(row[7]),
            "max_ingested_at_utc": str(row[8])
        }

    cursor.close()
    duration = time.time() - t0
    res = {
        "execution_time_utc": exec_time_utc,
        "available_databases": dbs,
        "gold_database_exists": gold_exists,
        "gold_database_location": gold_location,
        "tables": tables_meta,
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] snapshot: gold_exists={gold_exists}, tables_audited={len(tables_meta)}")
    logger.info(f"[END] section=snapshot (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 2: PROPONENTE (FINDING A)
# ==============================================================================
def run_proponente_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=proponente")
    t0 = time.time()
    cursor = conn.cursor()

    # Contagem geral
    log_query_start("Contagem global de proponentes em silver.siconv_proposta")
    t_q = time.time()
    cursor.execute("""
        SELECT
            COUNT(*) as total_propostas,
            COUNT(DISTINCT identificacao_proponente) as dist_proponentes,
            SUM(CASE WHEN identificacao_proponente IS NULL THEN 1 ELSE 0 END) as null_proponentes
        FROM silver.siconv_proposta
    """)
    tot_prop, dist_prop, null_prop = cursor.fetchone()
    log_query_end("Contagem global de proponentes em silver.siconv_proposta", t_q)

    attrs = [
        "nome_proponente",
        "codigo_municipio_ibge",
        "municipio_proponente",
        "uf_proponente",
        "cep_proponente",
        "endereco_proponente",
        "bairro_proponente",
        "natureza_juridica"
    ]

    attr_results = {}
    for attr in attrs:
        q_name = f"Analise de dependencia funcional identificacao_proponente -> {attr}"
        log_query_start(q_name)
        t_q = time.time()
        cursor.execute(f"""
            WITH prop_attr AS (
                SELECT
                    identificacao_proponente,
                    COUNT(DISTINCT {attr}) + MAX(CASE WHEN {attr} IS NULL THEN 1 ELSE 0 END) AS distinct_vals
                FROM silver.siconv_proposta
                WHERE identificacao_proponente IS NOT NULL
                GROUP BY identificacao_proponente
            )
            SELECT
                COUNT(*) AS total_proponentes,
                SUM(CASE WHEN distinct_vals = 1 THEN 1 ELSE 0 END) AS exact_1,
                SUM(CASE WHEN distinct_vals > 1 THEN 1 ELSE 0 END) AS more_than_1,
                MAX(distinct_vals) AS max_vals
            FROM prop_attr
        """)
        tot, exact_1, gt_1, max_val = cursor.fetchone()
        log_query_end(q_name, t_q)

        # Contagem de NULLs na coluna da tabela
        cursor.execute(f"SELECT COUNT(CASE WHEN {attr} IS NULL THEN 1 END) FROM silver.siconv_proposta")
        attr_nulls = cursor.fetchone()[0]

        pct_var = round((gt_1 / tot * 100.0), 4) if tot > 0 else 0.0

        # Maiores conflitos (se houver)
        conflicts = []
        if gt_1 > 0:
            cursor.execute(f"""
                WITH grp AS (
                    SELECT
                        identificacao_proponente,
                        COUNT(DISTINCT {attr}) + MAX(CASE WHEN {attr} IS NULL THEN 1 ELSE 0 END) AS distinct_vals
                    FROM silver.siconv_proposta
                    WHERE identificacao_proponente IS NOT NULL
                    GROUP BY identificacao_proponente
                    HAVING distinct_vals > 1
                    ORDER BY distinct_vals DESC
                    LIMIT 5
                )
                SELECT g.identificacao_proponente, g.distinct_vals
                FROM grp g
            """)
            conflicts = [{"identificacao_proponente": r[0], "distinct_vals": r[1]} for r in cursor.fetchall()]

        attr_results[attr] = {
            "total_proponentes": tot,
            "exact_1_value": exact_1,
            "more_than_1_value": gt_1,
            "pct_variation": pct_var,
            "max_distinct_values": max_val,
            "null_count_in_table": attr_nulls,
            "largest_conflicts": conflicts
        }

    # Teste da tupla completa
    q_name = "Analise de dependencia funcional sobre tupla cadastral completa"
    log_query_start(q_name)
    t_q = time.time()
    cursor.execute("""
        WITH distinct_tuples AS (
            SELECT
                identificacao_proponente,
                nome_proponente,
                codigo_municipio_ibge,
                municipio_proponente,
                uf_proponente,
                cep_proponente,
                endereco_proponente,
                bairro_proponente,
                natureza_juridica
            FROM silver.siconv_proposta
            WHERE identificacao_proponente IS NOT NULL
            GROUP BY
                identificacao_proponente,
                nome_proponente,
                codigo_municipio_ibge,
                municipio_proponente,
                uf_proponente,
                cep_proponente,
                endereco_proponente,
                bairro_proponente,
                natureza_juridica
        ),
        tuple_counts AS (
            SELECT
                identificacao_proponente,
                COUNT(*) as n_tuples
            FROM distinct_tuples
            GROUP BY identificacao_proponente
        )
        SELECT
            COUNT(*) as total_proponentes,
            SUM(CASE WHEN n_tuples = 1 THEN 1 ELSE 0 END) as exact_1_tuple,
            SUM(CASE WHEN n_tuples > 1 THEN 1 ELSE 0 END) as gt_1_tuple,
            MAX(n_tuples) as max_tuples
        FROM tuple_counts
    """)
    t_tot, t_exact, t_gt, t_max = cursor.fetchone()
    log_query_end(q_name, t_q)

    cursor.close()
    duration = time.time() - t0

    classification = "CONFIRMADA" if (t_gt == 0 and null_prop == 0) else "CANDIDATA COM RESTRIÇÕES"

    res = {
        "total_propostas": tot_prop,
        "distinct_identificacao_proponente": dist_prop,
        "null_identificacao_proponente": null_prop,
        "attributes": attr_results,
        "tuple_analysis": {
            "total_proponentes": t_tot,
            "exact_1_tuple": t_exact,
            "more_than_1_tuple": t_gt,
            "pct_variation": round((t_gt / t_tot * 100.0), 4) if t_tot > 0 else 0.0,
            "max_tuples_per_proponente": t_max
        },
        "classification": classification,
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] proponente: classification={classification}, distinct={dist_prop}, tuple_gt_1={t_gt}")
    logger.info(f"[END] section=proponente (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 3: MUNICÍPIO (FINDING B)
# ==============================================================================
def run_municipio_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=municipio")
    t0 = time.time()
    cursor = conn.cursor()

    log_query_start("Analise de dependencia funcional codigo_municipio_ibge -> (municipio, UF)")
    t_q = time.time()
    cursor.execute("""
        WITH mun_grp AS (
            SELECT
                codigo_municipio_ibge,
                COUNT(DISTINCT municipio_proponente) + MAX(CASE WHEN municipio_proponente IS NULL THEN 1 ELSE 0 END) as dist_mun,
                COUNT(DISTINCT uf_proponente) + MAX(CASE WHEN uf_proponente IS NULL THEN 1 ELSE 0 END) as dist_uf
            FROM silver.siconv_proposta
            WHERE codigo_municipio_ibge IS NOT NULL
            GROUP BY codigo_municipio_ibge
        )
        SELECT
            COUNT(*) as total_codigos,
            SUM(CASE WHEN dist_mun = 1 THEN 1 ELSE 0 END) as mun_exact_1,
            SUM(CASE WHEN dist_mun > 1 THEN 1 ELSE 0 END) as mun_gt_1,
            MAX(dist_mun) as mun_max,
            SUM(CASE WHEN dist_uf = 1 THEN 1 ELSE 0 END) as uf_exact_1,
            SUM(CASE WHEN dist_uf > 1 THEN 1 ELSE 0 END) as uf_gt_1,
            MAX(dist_uf) as uf_max
        FROM mun_grp
    """)
    tot_cd, m_ex1, m_gt1, m_max, u_ex1, u_gt1, u_max = cursor.fetchone()
    log_query_end("Analise de dependencia funcional codigo_municipio_ibge -> (municipio, UF)", t_q)

    # Verificação de nulos em codigo_municipio_ibge
    cursor.execute("SELECT COUNT(CASE WHEN codigo_municipio_ibge IS NULL THEN 1 END) FROM silver.siconv_proposta")
    null_ibge = cursor.fetchone()[0]

    # Teste da tupla (municipio, UF)
    log_query_start("Teste tupla (municipio, UF) por codigo_municipio_ibge")
    t_q = time.time()
    cursor.execute("""
        WITH distinct_tuples AS (
            SELECT codigo_municipio_ibge, municipio_proponente, uf_proponente
            FROM silver.siconv_proposta
            WHERE codigo_municipio_ibge IS NOT NULL
            GROUP BY codigo_municipio_ibge, municipio_proponente, uf_proponente
        ),
        tuple_counts AS (
            SELECT codigo_municipio_ibge, COUNT(*) as n_tuples
            FROM distinct_tuples
            GROUP BY codigo_municipio_ibge
        )
        SELECT
            COUNT(*) as total_codigos,
            SUM(CASE WHEN n_tuples = 1 THEN 1 ELSE 0 END) as exact_1_tuple,
            SUM(CASE WHEN n_tuples > 1 THEN 1 ELSE 0 END) as gt_1_tuple,
            MAX(n_tuples) as max_tuples
        FROM tuple_counts
    """)
    t_tot, t_ex1, t_gt1, t_max = cursor.fetchone()
    log_query_end("Teste tupla (municipio, UF) por codigo_municipio_ibge", t_q)

    cursor.close()
    duration = time.time() - t0

    classification = "CONFIRMADA" if (m_gt1 == 0 and u_gt1 == 0 and null_ibge == 0) else "RESTRITA"

    res = {
        "distinct_ibge_codes": tot_cd,
        "null_ibge_codes": null_ibge,
        "municipio_dependency": {
            "exact_1_name": m_ex1,
            "multiple_names": m_gt1,
            "max_names": m_max
        },
        "uf_dependency": {
            "exact_1_uf": u_ex1,
            "multiple_ufs": u_gt1,
            "max_ufs": u_max
        },
        "tuple_dependency": {
            "exact_1_tuple": t_ex1,
            "multiple_tuples": t_gt1,
            "max_tuples": t_max
        },
        "classification": classification,
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] municipio: classification={classification}, distinct_codes={tot_cd}, conflicts={m_gt1 + u_gt1}")
    logger.info(f"[END] section=municipio (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 4: ÓRGÃO (FINDING C)
# ==============================================================================
def run_orgao_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=orgao")
    t0 = time.time()
    cursor = conn.cursor()

    # 1. Dependências funcionais por papel
    tests = [
        ("orgao_superior_proposta", "silver.siconv_proposta", "codigo_orgao_superior", "descricao_orgao_superior"),
        ("orgao_concedente_proposta", "silver.siconv_proposta", "codigo_orgao", "descricao_orgao"),
        ("orgao_superior_programa", "silver.siconv_programa_cadastral", "codigo_orgao_superior_programa", "descricao_orgao_superior_programa")
    ]
    role_results = {}
    for role_name, tbl, cd_col, ds_col in tests:
        q_name = f"Dependencia funcional {role_name} ({cd_col} -> {ds_col})"
        log_query_start(q_name)
        t_q = time.time()
        cursor.execute(f"""
            WITH grp AS (
                SELECT
                    {cd_col} as cd,
                    COUNT(DISTINCT {ds_col}) + MAX(CASE WHEN {ds_col} IS NULL THEN 1 ELSE 0 END) as dist_desc
                FROM {tbl}
                WHERE {cd_col} IS NOT NULL
                GROUP BY {cd_col}
            )
            SELECT
                COUNT(*) as total_codigos,
                SUM(CASE WHEN dist_desc = 1 THEN 1 ELSE 0 END) as exact_1_desc,
                SUM(CASE WHEN dist_desc > 1 THEN 1 ELSE 0 END) as gt_1_desc,
                MAX(dist_desc) as max_desc
            FROM grp
        """)
        tot_cd, ex1, gt1, mx = cursor.fetchone()
        log_query_end(q_name, t_q)

        role_results[role_name] = {
            "table": tbl,
            "code_col": cd_col,
            "desc_col": ds_col,
            "distinct_codes": tot_cd,
            "exact_1_desc": ex1,
            "multiple_desc": gt1,
            "max_desc": mx
        }

    # 2. Comparação de universos
    log_query_start("Comparacao de universos de codigos de orgao")
    t_q = time.time()
    cursor.execute("""
        WITH sup AS (
            SELECT DISTINCT codigo_orgao_superior as cd, descricao_orgao_superior as ds
            FROM silver.siconv_proposta
        ),
        conc AS (
            SELECT DISTINCT codigo_orgao as cd, descricao_orgao as ds
            FROM silver.siconv_proposta
        ),
        prog AS (
            SELECT DISTINCT codigo_orgao_superior_programa as cd, descricao_orgao_superior_programa as ds
            FROM silver.siconv_programa_cadastral
        )
        SELECT
            (SELECT COUNT(DISTINCT cd) FROM sup) as n_sup,
            (SELECT COUNT(DISTINCT cd) FROM conc) as n_conc,
            (SELECT COUNT(DISTINCT cd) FROM prog) as n_prog,
            (SELECT COUNT(DISTINCT s.cd) FROM sup s JOIN conc c ON s.cd = c.cd) as n_sup_inter_conc,
            (SELECT COUNT(DISTINCT s.cd) FROM sup s JOIN prog p ON s.cd = p.cd) as n_sup_inter_prog,
            (SELECT COUNT(DISTINCT c.cd) FROM conc c JOIN prog p ON c.cd = p.cd) as n_conc_inter_prog
    """)
    n_sup, n_conc, n_prog, inter_sup_conc, inter_sup_prog, inter_conc_prog = cursor.fetchone()
    log_query_end("Comparacao de universos de codigos de orgao", t_q)

    # 3. Verificação de conflito de descrições entre papéis
    log_query_start("Verifica se descricoes divergem quando codigos coincidem")
    t_q = time.time()
    cursor.execute("""
        WITH sup AS (
            SELECT DISTINCT codigo_orgao_superior as cd, descricao_orgao_superior as ds
            FROM silver.siconv_proposta
        ),
        conc AS (
            SELECT DISTINCT codigo_orgao as cd, descricao_orgao as ds
            FROM silver.siconv_proposta
        )
        SELECT s.cd, s.ds as ds_sup, c.ds as ds_conc
        FROM sup s JOIN conc c ON s.cd = c.cd
        WHERE s.ds != c.ds
    """)
    diffs_sup_conc = cursor.fetchall()

    cursor.execute("""
        WITH sup AS (
            SELECT DISTINCT codigo_orgao_superior as cd, descricao_orgao_superior as ds
            FROM silver.siconv_proposta
        ),
        prog AS (
            SELECT DISTINCT codigo_orgao_superior_programa as cd, descricao_orgao_superior_programa as ds
            FROM silver.siconv_programa_cadastral
        )
        SELECT s.cd, s.ds as ds_sup, p.ds as ds_prog
        FROM sup s JOIN prog p ON s.cd = p.cd
        WHERE s.ds != p.ds
    """)
    diffs_sup_prog = cursor.fetchall()
    log_query_end("Verifica se descricoes divergem quando codigos coincidem", t_q)

    cursor.close()
    duration = time.time() - t0

    # Decisão de conformação
    conformed_safe = (
        len(diffs_sup_conc) == 0 and
        len(diffs_sup_prog) == 0 and
        role_results["orgao_superior_proposta"]["multiple_desc"] == 0 and
        role_results["orgao_concedente_proposta"]["multiple_desc"] == 0 and
        role_results["orgao_superior_programa"]["multiple_desc"] == 0
    )

    res = {
        "roles": role_results,
        "universes": {
            "n_superior_proposta": n_sup,
            "n_concedente_proposta": n_conc,
            "n_superior_programa": n_prog,
            "intersection_superior_concedente": inter_sup_conc,
            "intersection_superior_prop_prog": inter_sup_prog,
            "intersection_concedente_prog": inter_conc_prog
        },
        "description_mismatches_sup_vs_conc": len(diffs_sup_conc),
        "description_mismatches_sup_prop_vs_prog": len(diffs_sup_prog),
        "conformed_dim_orgao_defensible": conformed_safe,
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] orgao: conformed_safe={conformed_safe}, sup={n_sup}, conc={n_conc}, inter={inter_sup_conc}")
    logger.info(f"[END] section=orgao (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 5: CONVÊNIO (FINDING D)
# ==============================================================================
def run_convenio_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=convenio")
    t0 = time.time()
    cursor = conn.cursor()

    log_query_start("Busca schema e contagens gerais de silver.siconv_convenio")
    t_q = time.time()
    cursor.execute("DESCRIBE silver.siconv_convenio")
    cols = [r[0].strip() for r in cursor.fetchall() if r[0] and not r[0].startswith("#")]

    cursor.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT numero_convenio) as distinct_convenios
        FROM silver.siconv_convenio
    """)
    tot_rows, dist_conv = cursor.fetchone()
    log_query_end("Busca schema e contagens gerais de silver.siconv_convenio", t_q)

    # Identificar chaves com multiplicidade > 1
    cursor.execute("""
        SELECT numero_convenio, COUNT(*) as cnt
        FROM silver.siconv_convenio
        GROUP BY numero_convenio
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, numero_convenio
    """)
    dup_rows = cursor.fetchall()
    dup_convenios = [{"numero_convenio": r[0], "count": r[1]} for r in dup_rows]

    # Analisar estabilidade de cada coluna analítica
    tech_cols = {
        "id_convenio_observacao",
        "source_conflict_count",
        "has_source_conflict",
        "__ingested_at_utc",
        "__ingestion_run_id",
        "__source_file",
        "__source_sha256"
    }
    business_cols = [c for c in cols if c not in tech_cols]

    column_stability = {}
    for col in business_cols:
        q_name = f"Estabilidade da coluna {col} por numero_convenio"
        log_query_start(q_name)
        t_q = time.time()
        cursor.execute(f"""
            WITH col_grp AS (
                SELECT
                    numero_convenio,
                    COUNT(DISTINCT {col}) + MAX(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as dist_vals
                FROM silver.siconv_convenio
                GROUP BY numero_convenio
            )
            SELECT
                SUM(CASE WHEN dist_vals > 1 THEN 1 ELSE 0 END) as convenios_with_variation,
                MAX(dist_vals) as max_distinct_values
            FROM col_grp
        """)
        n_var, max_dist = cursor.fetchone()
        log_query_end(q_name, t_q)

        pct_var = round((n_var / dist_conv * 100.0), 6) if dist_conv > 0 else 0.0
        status = "ESTAVEL POR CONVENIO" if n_var == 0 else "VARIAVEL POR OBSERVACAO"

        examples = []
        if n_var > 0:
            cursor.execute(f"""
                SELECT numero_convenio, {col}
                FROM silver.siconv_convenio
                WHERE numero_convenio IN ('949286', '956078')
                ORDER BY numero_convenio, {col}
            """)
            examples = [{"numero_convenio": r[0], "value": str(r[1])} for r in cursor.fetchall()]

        column_stability[col] = {
            "status": status,
            "convenios_with_variation": n_var,
            "pct_variation": pct_var,
            "max_distinct_values": max_dist,
            "examples": examples
        }

    # Gate para fct_convenio
    varying_cols = [c for c, p in column_stability.items() if p["status"] == "VARIAVEL POR OBSERVACAO"]
    only_saldo_varies = (varying_cols == ["valor_saldo_conta"])
    all_other_stable = all(column_stability[c]["status"] == "ESTAVEL POR CONVENIO" for c in business_cols if c != "valor_saldo_conta")

    gate_fct_convenio = "CONFIRMADA" if (only_saldo_varies and all_other_stable) else "BLOQUEADA"

    cursor.close()
    duration = time.time() - t0

    res = {
        "total_rows_silver": tot_rows,
        "distinct_numero_convenio": dist_conv,
        "duplicate_numero_convenio_count": len(dup_convenios),
        "duplicate_convenios": dup_convenios,
        "business_columns_analyzed": len(business_cols),
        "varying_columns": varying_cols,
        "only_saldo_conta_varies": only_saldo_varies,
        "fct_convenio_gate": gate_fct_convenio,
        "recommended_fact_structure": {
            "fct_convenio": "1 linha por numero_convenio (sem valor_saldo_conta canonico)",
            "fct_convenio_saldo_observacao": "1 linha por id_convenio_observacao (preserva observacoes de saldo)"
        },
        "column_stability": column_stability,
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] convenio: gate={gate_fct_convenio}, distinct={dist_conv}, varying_cols={varying_cols}")
    logger.info(f"[END] section=convenio (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 6: PROGRAMA N:N (FINDING E)
# ==============================================================================
def run_programa_nn_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=programa-nn")
    t0 = time.time()
    cursor = conn.cursor()

    log_query_start("Distribuicao de multiplicidade de programas por proposta")
    t_q = time.time()
    cursor.execute("""
        WITH p_cnt AS (
            SELECT id_proposta, count(DISTINCT id_programa) as prog_cnt
            FROM silver.siconv_programa_proposta
            GROUP BY id_proposta
        ),
        prop_stats AS (
            SELECT p.id_proposta, COALESCE(c.prog_cnt, 0) as prog_cnt, p.valor_global_proposta
            FROM silver.siconv_proposta p
            LEFT JOIN p_cnt c ON p.id_proposta = c.id_proposta
        )
        SELECT
            prog_cnt,
            COUNT(*) as num_propostas,
            SUM(valor_global_proposta) as sum_valor_unico,
            SUM(valor_global_proposta * prog_cnt) as sum_valor_inflado
        FROM prop_stats
        GROUP BY prog_cnt
        ORDER BY prog_cnt
    """)
    prop_rows = cursor.fetchall()
    log_query_end("Distribuicao de multiplicidade de programas por proposta", t_q)

    prop_dist = []
    tot_prop_unique_val = Decimal("0")
    tot_prop_inflated_val = Decimal("0")
    propostas_sem_programa = 0
    propostas_1_programa = 0
    propostas_gt_1_programa = 0
    max_prog_per_prop = 0

    for r in prop_rows:
        cnt = r[0]
        n_p = r[1]
        v_u = Decimal(str(r[2])) if r[2] is not None else Decimal("0")
        v_inf = Decimal(str(r[3])) if r[3] is not None else Decimal("0")

        prop_dist.append({
            "program_count": cnt,
            "propostas_count": n_p,
            "sum_valor_global_unico": v_u,
            "sum_valor_global_inflado": v_inf
        })

        if cnt == 0:
            propostas_sem_programa += n_p
        elif cnt == 1:
            propostas_1_programa += n_p
            tot_prop_unique_val += v_u
            tot_prop_inflated_val += v_inf
            if cnt > max_prog_per_prop:
                max_prog_per_prop = cnt
        else:
            propostas_gt_1_programa += n_p
            tot_prop_unique_val += v_u
            tot_prop_inflated_val += v_inf
            if cnt > max_prog_per_prop:
                max_prog_per_prop = cnt

    prop_inflation_diff = tot_prop_inflated_val - tot_prop_unique_val
    prop_inflation_factor = (
        float(tot_prop_inflated_val / tot_prop_unique_val) if tot_prop_unique_val > 0 else 1.0
    )

    # Simulação para convênios
    log_query_start("Distribuicao de multiplicidade de programas por convenio")
    t_q = time.time()
    cursor.execute("""
        WITH p_cnt AS (
            SELECT id_proposta, count(DISTINCT id_programa) as prog_cnt
            FROM silver.siconv_programa_proposta
            GROUP BY id_proposta
        ),
        conv_unique AS (
            SELECT numero_convenio, id_proposta, valor_global_convenio
            FROM silver.siconv_convenio
            GROUP BY numero_convenio, id_proposta, valor_global_convenio
        ),
        conv_stats AS (
            SELECT cv.numero_convenio, COALESCE(c.prog_cnt, 0) as prog_cnt, cv.valor_global_convenio
            FROM conv_unique cv
            LEFT JOIN p_cnt c ON cv.id_proposta = c.id_proposta
        )
        SELECT
            prog_cnt,
            COUNT(*) as num_convenios,
            SUM(valor_global_convenio) as sum_valor_unico,
            SUM(valor_global_convenio * prog_cnt) as sum_valor_inflado
        FROM conv_stats
        GROUP BY prog_cnt
        ORDER BY prog_cnt
    """)
    conv_rows = cursor.fetchall()
    log_query_end("Distribuicao de multiplicidade de programas por convenio", t_q)

    conv_dist = []
    tot_conv_unique_val = Decimal("0")
    tot_conv_inflated_val = Decimal("0")
    for r in conv_rows:
        cnt = r[0]
        n_c = r[1]
        v_u = Decimal(str(r[2])) if r[2] is not None else Decimal("0")
        v_inf = Decimal(str(r[3])) if r[3] is not None else Decimal("0")
        conv_dist.append({
            "program_count": cnt,
            "convenios_count": n_c,
            "sum_valor_global_unico": v_u,
            "sum_valor_global_inflado": v_inf
        })
        if cnt >= 1:
            tot_conv_unique_val += v_u
            tot_conv_inflated_val += v_inf

    conv_inflation_diff = tot_conv_inflated_val - tot_conv_unique_val
    conv_inflation_factor = (
        float(tot_conv_inflated_val / tot_conv_unique_val) if tot_conv_unique_val > 0 else 1.0
    )

    cursor.close()
    duration = time.time() - t0

    res = {
        "proposta_summary": {
            "propostas_sem_programa": propostas_sem_programa,
            "propostas_exatamente_1_programa": propostas_1_programa,
            "propostas_mais_de_1_programa": propostas_gt_1_programa,
            "max_programas_por_proposta": max_prog_per_prop,
            "valor_unico_vinculado": tot_prop_unique_val,
            "valor_inflado_direto": tot_prop_inflated_val,
            "diferenca_inflacao": prop_inflation_diff,
            "fator_inflacao": round(prop_inflation_factor, 6),
            "distribution": prop_dist
        },
        "convenio_summary": {
            "valor_unico_vinculado": tot_conv_unique_val,
            "valor_inflado_direto": tot_conv_inflated_val,
            "diferenca_inflacao": conv_inflation_diff,
            "fator_inflacao": round(conv_inflation_factor, 6),
            "distribution": conv_dist
        },
        "contract_rules": {
            "allowed_metrics": [
                "COUNT(DISTINCT id_proposta) por programa (quantidade de propostas associadas)"
            ],
            "attention_metrics": [
                "COUNT(DISTINCT id_convenio) por programa (instrumentos associados, nao aditiva entre programas)"
            ],
            "forbidden_additive_metrics": [
                "SUM(valor_global_proposta) originada diretamente de join com a ponte de programa",
                "SUM(valor_global_convenio) originada diretamente de join com a ponte de programa"
            ],
            "proportional_allocation": "NAO APROVADA"
        },
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] programa-nn: sem_prog={propostas_sem_programa}, 1_prog={propostas_1_programa}, gt_1_prog={propostas_gt_1_programa}, prop_inflacao=+R${prop_inflation_diff}")
    logger.info(f"[END] section=programa-nn (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 7: DATAS (FINDING F)
# ==============================================================================
def run_datas_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=datas")
    t0 = time.time()
    cursor = conn.cursor()

    date_targets = [
        # Proposta
        ("silver.siconv_proposta", "data_proposta", "proposta"),
        ("silver.siconv_proposta", "data_inicio_vigencia_proposta", "proposta"),
        ("silver.siconv_proposta", "data_fim_vigencia_proposta", "proposta"),
        # Convênio
        ("silver.siconv_convenio", "data_assinatura_convenio", "convenio"),
        ("silver.siconv_convenio", "data_publicacao_convenio", "convenio"),
        ("silver.siconv_convenio", "data_inicio_vigencia_convenio", "convenio"),
        ("silver.siconv_convenio", "data_fim_vigencia_convenio", "convenio"),
        ("silver.siconv_convenio", "data_fim_vigencia_original", "convenio"),
        ("silver.siconv_convenio", "data_limite_prestacao_contas", "convenio"),
        ("silver.siconv_convenio", "data_suspensiva", "convenio"),
        ("silver.siconv_convenio", "data_retirada_suspensiva", "convenio"),
        # Programa Cadastral
        ("silver.siconv_programa_cadastral", "data_disponibilizacao", "programa_cadastral"),
        # Programa Elegibilidade
        ("silver.siconv_programa_elegibilidade", "data_disponibilizacao", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_inicio_recebimento_proposta", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_fim_recebimento_proposta", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_inicio_emenda_parlamentar", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_fim_emenda_parlamentar", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_inicio_beneficiario_especifico", "programa_elegibilidade"),
        ("silver.siconv_programa_elegibilidade", "data_fim_beneficiario_especifico", "programa_elegibilidade")
    ]

    date_profiles = {}
    for tbl, col, domain in date_targets:
        key = f"{tbl}.{col}"
        q_name = f"Perfil temporal de {key}"
        log_query_start(q_name)
        t_q = time.time()
        cursor.execute(f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count,
                MIN({col}) as min_val,
                MAX({col}) as max_val,
                COUNT(DISTINCT YEAR({col})) as distinct_years,
                SUM(CASE WHEN YEAR({col}) < 1990 THEN 1 ELSE 0 END) as lt_1990,
                SUM(CASE WHEN YEAR({col}) > 2050 THEN 1 ELSE 0 END) as gt_2050
            FROM {tbl}
        """)
        tot, n_null, mn, mx, dist_yr, lt_90, gt_50 = cursor.fetchone()
        log_query_end(q_name, t_q)

        # Exemplos anômalos
        anomalies = []
        if (lt_90 + gt_50) > 0:
            cursor.execute(f"""
                SELECT DISTINCT {col}
                FROM {tbl}
                WHERE {col} IS NOT NULL AND (YEAR({col}) < 1990 OR YEAR({col}) > 2050)
                ORDER BY {col}
                LIMIT 5
            """)
            anomalies = [str(r[0]) for r in cursor.fetchall()]

        pct_null = round((n_null / tot * 100.0), 4) if tot > 0 else 0.0

        date_profiles[key] = {
            "table": tbl,
            "column": col,
            "domain": domain,
            "total_rows": tot,
            "null_count": n_null,
            "pct_null": pct_null,
            "min_date": str(mn),
            "max_date": str(mx),
            "distinct_years": dist_yr,
            "count_before_1990": lt_90,
            "count_after_2050": gt_50,
            "out_of_bounds_examples": anomalies
        }

    cursor.close()
    duration = time.time() - t0

    res = {
        "total_date_fields_analyzed": len(date_targets),
        "fields": date_profiles,
        "candidate_dim_data_contract": {
            "grain": "1 dia calendario",
            "primary_key": "data_sk (formato YYYYMMDD)",
            "role_playing": "Multiplos papeis analiticos (data_proposta, data_assinatura, data_publicacao, vigencia, etc.)",
            "candidate_domain": "1990-01-01 a 2050-12-31",
            "special_keys": {
                "-1": "Data nao informada / NULL",
                "-2": "Data fora do dominio analitico valido (<1990 ou >2050)"
            }
        },
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] datas: fields_profiled={len(date_targets)}")
    logger.info(f"[END] section=datas (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# SEÇÃO 8: MÉTRICAS (FINDINGS G, H, I)
# ==============================================================================
def run_metricas_section(conn) -> Dict[str, Any]:
    logger.info("[START] section=metricas")
    t0 = time.time()
    cursor = conn.cursor()

    # 1. Medidas de Proposta
    log_query_start("Calculo baseline de medidas em silver.siconv_proposta")
    t_q = time.time()
    cursor.execute("""
        SELECT
            COUNT(*) as total_propostas,
            SUM(valor_global_proposta) as sum_global,
            SUM(valor_repasse_proposta) as sum_repasse,
            SUM(valor_contrapartida_proposta) as sum_contrapartida
        FROM silver.siconv_proposta
    """)
    n_prop, prop_global, prop_repasse, prop_contrapartida = cursor.fetchone()
    log_query_end("Calculo baseline de medidas em silver.siconv_proposta", t_q)

    # 2. Medidas de Convênio (canônicas distinct por numero_convenio)
    log_query_start("Calculo baseline de medidas em silver.siconv_convenio")
    t_q = time.time()
    cursor.execute("""
        WITH u AS (
            SELECT
                numero_convenio,
                valor_global_convenio,
                valor_repasse_convenio,
                valor_contrapartida_convenio,
                valor_empenhado_convenio,
                valor_desembolsado_convenio,
                valor_saldo_remanescente_tesouro,
                valor_saldo_remanescente_convenente,
                valor_rendimento_aplicacao,
                valor_ingresso_contrapartida,
                valor_global_original_convenio
            FROM silver.siconv_convenio
            GROUP BY
                numero_convenio,
                valor_global_convenio,
                valor_repasse_convenio,
                valor_contrapartida_convenio,
                valor_empenhado_convenio,
                valor_desembolsado_convenio,
                valor_saldo_remanescente_tesouro,
                valor_saldo_remanescente_convenente,
                valor_rendimento_aplicacao,
                valor_ingresso_contrapartida,
                valor_global_original_convenio
        )
        SELECT
            COUNT(*) as distinct_convenios,
            SUM(valor_global_convenio) as sum_global,
            SUM(valor_repasse_convenio) as sum_repasse,
            SUM(valor_contrapartida_convenio) as sum_contrapartida,
            SUM(valor_empenhado_convenio) as sum_empenhado,
            SUM(valor_desembolsado_convenio) as sum_desembolsado,
            SUM(valor_saldo_remanescente_tesouro) as sum_saldo_tesouro,
            SUM(valor_saldo_remanescente_convenente) as sum_saldo_convenente,
            SUM(valor_rendimento_aplicacao) as sum_rendimento,
            SUM(valor_ingresso_contrapartida) as sum_ingresso_cp,
            SUM(valor_global_original_convenio) as sum_global_original
        FROM u
    """)
    (n_conv, conv_global, conv_repasse, conv_contrapartida, conv_empenhado,
     conv_desembolsado, conv_saldo_tesouro, conv_saldo_convenente, conv_rendimento,
     conv_ingresso_cp, conv_global_orig) = cursor.fetchone()
    log_query_end("Calculo baseline de medidas em silver.siconv_convenio", t_q)

    # 3. Taxa de conveniação
    log_query_start("Calculo da taxa de conveniacao")
    t_q = time.time()
    cursor.execute("""
        WITH c_prop AS (
            SELECT COUNT(DISTINCT id_proposta) as propostas_conveniadas
            FROM silver.siconv_convenio
        )
        SELECT propostas_conveniadas FROM c_prop
    """)
    propostas_conveniadas = cursor.fetchone()[0]
    log_query_end("Calculo da taxa de conveniacao", t_q)

    taxa_conv_pct = round(float(propostas_conveniadas) / float(n_prop) * 100.0, 4) if n_prop > 0 else 0.0

    # 4. Indicadores derivados
    ticket_medio_prop = round(float(prop_global / Decimal(n_prop)), 2) if n_prop > 0 else 0.0
    ticket_medio_conv = round(float(conv_global / Decimal(n_conv)), 2) if n_conv > 0 else 0.0
    pct_empenhado_repasse = round(float(conv_empenhado / conv_repasse) * 100.0, 4) if conv_repasse > 0 else 0.0
    pct_desembolsado_repasse = round(float(conv_desembolsado / conv_repasse) * 100.0, 4) if conv_repasse > 0 else 0.0

    cursor.close()
    duration = time.time() - t0

    res = {
        "base_metrics": {
            "quantidade_propostas": n_prop,
            "valor_global_propostas": prop_global,
            "valor_repasse_propostas": prop_repasse,
            "valor_contrapartida_propostas": prop_contrapartida,
            "quantidade_convenios_canonicos": n_conv,
            "valor_global_convenios": conv_global,
            "valor_repasse_convenios": conv_repasse,
            "valor_contrapartida_convenios": conv_contrapartida,
            "valor_empenhado_convenios": conv_empenhado,
            "valor_desembolsado_convenios": conv_desembolsado,
            "valor_saldo_remanescente_tesouro": conv_saldo_tesouro,
            "valor_saldo_remanescente_convenente": conv_saldo_convenente,
            "valor_rendimento_aplicacao": conv_rendimento,
            "valor_ingresso_contrapartida": conv_ingresso_cp,
            "valor_global_original_convenios": conv_global_orig
        },
        "derived_metrics": {
            "taxa_conveniacao_pct": taxa_conv_pct,
            "propostas_conveniadas": propostas_conveniadas,
            "ticket_medio_proposta": ticket_medio_prop,
            "ticket_medio_convenio": ticket_medio_conv,
            "percentual_empenhado_sobre_repasse": pct_empenhado_repasse,
            "percentual_desembolsado_sobre_repasse": pct_desembolsado_repasse
        },
        "duration_seconds": round(duration, 2)
    }
    logger.info(f"[RESULT] metricas: taxa_conv={taxa_conv_pct}%, ticket_prop={ticket_medio_prop}, ticket_conv={ticket_medio_conv}, empenhado={pct_empenhado_repasse}%")
    logger.info(f"[END] section=metricas (duracao: {duration:.2f}s)")
    return res

# ==============================================================================
# MAIN / CLI
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Profiling Dirigido R4-A (Gold Discovery & Contracts)")
    parser.add_argument(
        "--section",
        choices=["snapshot", "proponente", "municipio", "orgao", "convenio", "programa-nn", "datas", "metricas", "all"],
        default="all",
        help="Secao analitica a ser executada"
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Caminho para salvar os resultados consolidados em formato JSON"
    )
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Spark Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Spark Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuario de conexao")

    args = parser.parse_args()

    logger.info(f"[START] Execucao do profiling R4-A (secao: {args.section})")
    conn = get_connection(host=args.host, port=args.port, user=args.user)

    full_results = {}
    try:
        if args.section in ("snapshot", "all"):
            full_results["snapshot"] = run_snapshot_section(conn)

        if args.section in ("proponente", "all"):
            full_results["proponente"] = run_proponente_section(conn)

        if args.section in ("municipio", "all"):
            full_results["municipio"] = run_municipio_section(conn)

        if args.section in ("orgao", "all"):
            full_results["orgao"] = run_orgao_section(conn)

        if args.section in ("convenio", "all"):
            full_results["convenio"] = run_convenio_section(conn)

        if args.section in ("programa-nn", "all"):
            full_results["programa_nn"] = run_programa_nn_section(conn)

        if args.section in ("datas", "all"):
            full_results["datas"] = run_datas_section(conn)

        if args.section in ("metricas", "all"):
            full_results["metricas"] = run_metricas_section(conn)

    finally:
        conn.close()

    if args.output_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_json)), exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(full_results, f, indent=2, cls=DecimalEncoder, ensure_ascii=False)
        logger.info(f"Resultados salvos com sucesso em: {args.output_json}")

    logger.info(f"[END] Profiling R4-A concluido com sucesso.")

if __name__ == "__main__":
    main()
