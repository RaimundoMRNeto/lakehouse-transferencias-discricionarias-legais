#!/usr/bin/env python3
"""
Script de reconciliação dinâmica Silver -> Gold (R4-B).
Executa auditoria estrita, somente leitura e distribuída no Spark Thrift Server:
1. gold_location: Conformidade do location físico da database gold.
2. row_counts: Reconciliação dinâmica de volumes entre Silver e Gold.
3. primary_keys: Unicidade e não-nulidade das PKs (e grão 1:1 de convênio->proposta).
4. referential_integrity: Integridade referencial entre fatos, dimensões e bridge (com exceções conhecidas).
5. date_mapping: Mapeamento de chaves de calendário e sentinelas (-1, -2).
6. orgao_conformance: Prova de conformação institucional dos órgãos na Silver.
7. convenio_canonical_stability: Prova de estabilidade da assinatura de atributos de convênio.
8. financials_proposta: Reconciliação financeira exata em DECIMAL(38,2) (tolerância R$ 0,00).
9. financials_convenio: Reconciliação financeira canônica exata em DECIMAL(38,2) (tolerância R$ 0,00).
10. saldo_observations: Preservação de observações físicas e checksum técnico de saldo.
11. bridge_guardrails: Verificação de ausência de métricas monetárias ou rateios na bridge.

Modo padrão: 100% dinâmico contra os snapshots correntes.
Modo baseline: --check-baseline-r4a valida contra os números históricos de 16/09/2026.
"""
import sys
import time
import argparse
import logging
from decimal import Decimal
from typing import Dict, Any, List, Tuple, Set, Optional
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("reconcile_silver_gold")

KNOWN_BRIDGE_ORPHAN_PROPOSALS: Set[int] = {321453, 1427146, 296629}

PROHIBITED_BRIDGE_COLUMNS: Set[str] = {
    "valor_global_proposta",
    "valor_repasse_proposta",
    "valor_contrapartida_proposta",
    "valor_global_convenio",
    "valor_repasse_convenio",
    "valor_contrapartida_convenio",
    "valor_alocado",
    "valor_saldo_conta",
    "peso",
    "percentual",
    "rateio",
    "valor",
    "share"
}

HISTORICAL_BASELINE_R4A = {
    "row_counts": {
        "dim_data": 22282,
        "dim_proponente": 29325,
        "dim_municipio": 5570,
        "dim_orgao": 184,
        "dim_programa": 53018,
        "fct_proposta": 1157619,
        "bridge_programa_proposta": 1158975,
        "fct_convenio": 287584,
        "fct_convenio_saldo_observacao": 287586,
    },
    "financials_proposta": {
        "valor_global": Decimal("1495209875334.42"),
        "valor_repasse": Decimal("1425758135735.63"),
        "valor_contrapartida": Decimal("69451779598.79"),
    },
    "financials_convenio_canonical": {
        "valor_global": Decimal("356840744533.16"),
        "valor_repasse": Decimal("331271766468.34"),
        "valor_contrapartida": Decimal("23709764114.58"),
        "valor_empenhado": Decimal("192064543060.79"),
        "valor_desembolsado": Decimal("153205012397.29"),
    },
    "saldo_checksum": Decimal("18079072724.97"),
}


def normalize_location(loc: Optional[str]) -> str:
    """Normaliza URI de storage removendo barras finais e espaços."""
    if not loc:
        return ""
    return str(loc).strip().rstrip("/")


def get_connection(host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
    conn = hive.Connection(host=host, port=port, username=user)
    cur = conn.cursor()
    cur.execute("SET spark.sql.autoBroadcastJoinThreshold = -1")
    return conn


def run_scalar_query(cur, query: str) -> Any:
    cur.execute(query)
    res = cur.fetchall()
    if res and len(res) > 0:
        return res[0][0]
    return None


def run_row_query(cur, query: str) -> Tuple:
    cur.execute(query)
    res = cur.fetchall()
    if res and len(res) > 0:
        return res[0]
    return ()


def check_gold_location(cur, expected_location: str = "s3a://gold/warehouse") -> bool:
    logger.info("=" * 70)
    logger.info("1. VERIFICAÇÃO DE LOCATION FÍSICO DO SCHEMA GOLD")
    logger.info("=" * 70)

    cur.execute("DESCRIBE DATABASE EXTENDED gold")
    rows = cur.fetchall()
    actual_loc = None
    for row in rows:
        if len(row) >= 2 and str(row[0]).strip().lower() == "location":
            actual_loc = str(row[1]).strip()
            break

    expected_norm = normalize_location(expected_location)
    actual_norm = normalize_location(actual_loc)

    is_ok = (actual_norm == expected_norm)
    status = "PASS" if is_ok else "FAIL"
    logger.info(f"[{status}] gold location: Esperado='{expected_location}' | Atual='{actual_loc}'")
    return is_ok


def reconcile_row_counts(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 70)
    logger.info("2. RECONCILIAÇÃO DINÂMICA DE CONTAGEM DE LINHAS (ROW COUNTS)")
    logger.info("=" * 70)

    tables = [
        ("dim_data", "SELECT 22282", "SELECT count(*) FROM gold.dim_data"),
        ("dim_proponente", "SELECT count(distinct identificacao_proponente) FROM silver.siconv_proposta", "SELECT count(*) FROM gold.dim_proponente"),
        ("dim_municipio", "SELECT count(distinct codigo_municipio_ibge) FROM silver.siconv_proposta", "SELECT count(*) FROM gold.dim_municipio"),
        ("dim_orgao", """
            SELECT count(distinct cd) FROM (
                SELECT codigo_orgao_superior as cd FROM silver.siconv_proposta
                UNION
                SELECT codigo_orgao as cd FROM silver.siconv_proposta
                UNION
                SELECT codigo_orgao_superior_programa as cd FROM silver.siconv_programa_cadastral
            )
        """, "SELECT count(*) FROM gold.dim_orgao"),
        ("dim_programa", "SELECT count(distinct id_programa) FROM silver.siconv_programa_cadastral", "SELECT count(*) FROM gold.dim_programa"),
        ("fct_proposta", "SELECT count(*) FROM silver.siconv_proposta", "SELECT count(*) FROM gold.fct_proposta"),
        ("bridge_programa_proposta", "SELECT count(*) FROM silver.siconv_programa_proposta", "SELECT count(*) FROM gold.bridge_programa_proposta"),
        ("fct_convenio", """
            SELECT count(*) FROM (
                SELECT DISTINCT
                    numero_convenio, id_proposta, data_assinatura_convenio, data_publicacao_convenio,
                    data_inicio_vigencia_convenio, data_fim_vigencia_convenio, data_limite_prestacao_contas,
                    situacao_convenio, subsituacao_convenio, situacao_publicacao, situacao_contratacao,
                    is_instrumento_ativo, is_opera_obtv, is_assinado, numero_processo, unidade_gestora_emitente,
                    quantidade_termos_aditivos, quantidade_prorrogacoes, valor_global_convenio,
                    valor_repasse_convenio, valor_contrapartida_convenio, valor_empenhado_convenio,
                    valor_desembolsado_convenio, valor_saldo_remanescente_tesouro,
                    valor_saldo_remanescente_convenente, valor_rendimento_aplicacao,
                    valor_ingresso_contrapartida, valor_global_original_convenio
                FROM silver.siconv_convenio
            )
        """, "SELECT count(*) FROM gold.fct_convenio"),
        ("fct_convenio_saldo_observacao", "SELECT count(*) FROM silver.siconv_convenio", "SELECT count(*) FROM gold.fct_convenio_saldo_observacao"),
    ]

    all_passed = True
    for label, s_query, g_query in tables:
        t0 = time.time()
        s_cnt = run_scalar_query(cur, s_query)
        g_cnt = run_scalar_query(cur, g_query)
        elapsed = time.time() - t0

        diff = g_cnt - s_cnt
        status = "PASS" if diff == 0 else "FAIL"
        if diff != 0:
            all_passed = False

        baseline_info = ""
        if check_baseline:
            expected_base = HISTORICAL_BASELINE_R4A["row_counts"].get(label)
            if expected_base is not None:
                matches_base = (g_cnt == expected_base)
                if not matches_base:
                    all_passed = False
                    status = "FAIL"
                baseline_info = f" | Baseline={expected_base:,} ({'MATCH' if matches_base else 'MISMATCH'})"

        logger.info(f"[{status}] {label:30}: Silver/Canonical={s_cnt:,} | Gold={g_cnt:,} | Diff={diff:,}{baseline_info} ({elapsed:.2f}s)")

    return all_passed


def reconcile_primary_keys(cur) -> bool:
    logger.info("=" * 70)
    logger.info("3. UNICIDADE E NÃO-NULIDADE DE PRIMARY KEYS")
    logger.info("=" * 70)

    checks = [
        ("gold.dim_data (data_sk)", "gold.dim_data", "data_sk", "count(*)", "count(distinct data_sk)", "sum(case when data_sk is null then 1 else 0 end)"),
        ("gold.dim_proponente (identificacao_proponente)", "gold.dim_proponente", "identificacao_proponente", "count(*)", "count(distinct identificacao_proponente)", "sum(case when identificacao_proponente is null then 1 else 0 end)"),
        ("gold.dim_municipio (codigo_municipio_ibge)", "gold.dim_municipio", "codigo_municipio_ibge", "count(*)", "count(distinct codigo_municipio_ibge)", "sum(case when codigo_municipio_ibge is null then 1 else 0 end)"),
        ("gold.dim_orgao (codigo_orgao)", "gold.dim_orgao", "codigo_orgao", "count(*)", "count(distinct codigo_orgao)", "sum(case when codigo_orgao is null then 1 else 0 end)"),
        ("gold.dim_programa (id_programa)", "gold.dim_programa", "id_programa", "count(*)", "count(distinct id_programa)", "sum(case when id_programa is null then 1 else 0 end)"),
        ("gold.fct_proposta (id_proposta)", "gold.fct_proposta", "id_proposta", "count(*)", "count(distinct id_proposta)", "sum(case when id_proposta is null then 1 else 0 end)"),
        ("gold.fct_convenio (numero_convenio)", "gold.fct_convenio", "numero_convenio", "count(*)", "count(distinct numero_convenio)", "sum(case when numero_convenio is null then 1 else 0 end)"),
        ("gold.fct_convenio (id_proposta)", "gold.fct_convenio", "id_proposta", "count(*)", "count(distinct id_proposta)", "sum(case when id_proposta is null then 1 else 0 end)"),
        ("gold.fct_convenio_saldo_observacao (id_convenio_observacao)", "gold.fct_convenio_saldo_observacao", "id_convenio_observacao", "count(*)", "count(distinct id_convenio_observacao)", "sum(case when id_convenio_observacao is null then 1 else 0 end)"),
    ]

    all_passed = True
    for label, tbl, col, cnt_expr, dist_expr, null_expr in checks:
        t0 = time.time()
        row = run_row_query(cur, f"SELECT {cnt_expr}, {dist_expr}, {null_expr} FROM {tbl}")
        elapsed = time.time() - t0
        total, distinct, nulls = row[0], row[1], row[2]
        dups = total - distinct
        is_ok = (dups == 0 and nulls == 0)
        status = "PASS" if is_ok else "FAIL"
        if not is_ok:
            all_passed = False
        logger.info(f"[{status}] {label}: Total={total:,} | Distintos={distinct:,} | Duplicados={dups} | Nulos={nulls} ({elapsed:.2f}s)")

    # Unicidade do par composto na bridge
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM (
            SELECT id_programa, id_proposta, count(*) as cnt
            FROM gold.bridge_programa_proposta
            GROUP BY id_programa, id_proposta
            HAVING count(*) > 1
        ) t
    """)
    bridge_dup_pairs = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    is_ok = (bridge_dup_pairs == 0)
    status = "PASS" if is_ok else "FAIL"
    if not is_ok:
        all_passed = False
    logger.info(f"[{status}] gold.bridge_programa_proposta (id_programa, id_proposta): Pares duplicados={bridge_dup_pairs} ({elapsed:.2f}s)")

    return all_passed


def reconcile_referential_integrity(cur) -> bool:
    logger.info("=" * 70)
    logger.info("4. INTEGRIDADE REFERENCIAL E AUDITORIA DE ORFANDADE")
    logger.info("=" * 70)

    fk_checks = [
        ("dim_programa.codigo_orgao_superior_programa -> dim_orgao", """
            SELECT count(*) FROM gold.dim_programa p
            LEFT JOIN gold.dim_orgao o ON p.codigo_orgao_superior_programa = o.codigo_orgao
            WHERE o.codigo_orgao IS NULL AND p.codigo_orgao_superior_programa IS NOT NULL
        """),
        ("fct_proposta.identificacao_proponente -> dim_proponente", """
            SELECT count(*) FROM gold.fct_proposta f
            LEFT JOIN gold.dim_proponente d ON f.identificacao_proponente = d.identificacao_proponente
            WHERE d.identificacao_proponente IS NULL
        """),
        ("fct_proposta.codigo_municipio_ibge -> dim_municipio", """
            SELECT count(*) FROM gold.fct_proposta f
            LEFT JOIN gold.dim_municipio d ON f.codigo_municipio_ibge = d.codigo_municipio_ibge
            WHERE d.codigo_municipio_ibge IS NULL
        """),
        ("fct_proposta.codigo_orgao_superior -> dim_orgao", """
            SELECT count(*) FROM gold.fct_proposta f
            LEFT JOIN gold.dim_orgao d ON f.codigo_orgao_superior = d.codigo_orgao
            WHERE d.codigo_orgao IS NULL
        """),
        ("fct_proposta.codigo_orgao -> dim_orgao", """
            SELECT count(*) FROM gold.fct_proposta f
            LEFT JOIN gold.dim_orgao d ON f.codigo_orgao = d.codigo_orgao
            WHERE d.codigo_orgao IS NULL
        """),
        ("fct_convenio.id_proposta -> fct_proposta", """
            SELECT count(*) FROM gold.fct_convenio c
            LEFT JOIN gold.fct_proposta p ON c.id_proposta = p.id_proposta
            WHERE p.id_proposta IS NULL
        """),
        ("fct_convenio_saldo_observacao.numero_convenio -> fct_convenio", """
            SELECT count(*) FROM gold.fct_convenio_saldo_observacao s
            LEFT JOIN gold.fct_convenio c ON s.numero_convenio = c.numero_convenio
            WHERE c.numero_convenio IS NULL
        """),
        ("bridge_programa_proposta.id_programa -> dim_programa", """
            SELECT count(*) FROM gold.bridge_programa_proposta b
            LEFT JOIN gold.dim_programa p ON b.id_programa = p.id_programa
            WHERE p.id_programa IS NULL
        """),
    ]

    all_passed = True
    for label, query in fk_checks:
        t0 = time.time()
        orphans = run_scalar_query(cur, query)
        elapsed = time.time() - t0
        is_ok = (orphans == 0)
        status = "PASS" if is_ok else "FAIL"
        if not is_ok:
            all_passed = False
        logger.info(f"[{status}] FK {label}: Órfãos={orphans} ({elapsed:.2f}s)")

    # Auditoria da bridge com exceções conhecidas de propostas
    t0 = time.time()
    cur.execute("""
        SELECT distinct b.id_proposta
        FROM gold.bridge_programa_proposta b
        LEFT JOIN gold.fct_proposta p ON b.id_proposta = p.id_proposta
        WHERE p.id_proposta IS NULL
    """)
    observed_orphans = {row[0] for row in cur.fetchall()}
    elapsed = time.time() - t0

    unexpected_orphans = observed_orphans - KNOWN_BRIDGE_ORPHAN_PROPOSALS
    is_ok = (len(unexpected_orphans) == 0)
    status = "PASS" if is_ok else "FAIL"
    if not is_ok:
        all_passed = False

    logger.info(
        f"[{status}] Bridge -> Proposta (Órfãos): Observados={len(observed_orphans)} "
        f"(Conhecidos={len(observed_orphans & KNOWN_BRIDGE_ORPHAN_PROPOSALS)}, "
        f"Inesperados={len(unexpected_orphans)}) ({elapsed:.2f}s)"
    )
    if unexpected_orphans:
        logger.error(f"Novos órfãos inesperados detectados na bridge: {unexpected_orphans}")

    return all_passed


def reconcile_date_mapping(cur) -> bool:
    logger.info("=" * 70)
    logger.info("5. VALIDAÇÃO DE MAPEAMENTO TEMPORAL E SENTINELAS EM DIM_DATA")
    logger.info("=" * 70)

    # 1. Estrutura de dim_data
    t0 = time.time()
    row = run_row_query(cur, """
        SELECT
            count(*),
            sum(case when data_sk = -1 then 1 else 0 end),
            sum(case when data_sk = -2 then 1 else 0 end),
            sum(case when data_sk > 0 then 1 else 0 end),
            min(data),
            max(data)
        FROM gold.dim_data
    """)
    elapsed = time.time() - t0
    total, minus_one, minus_two, normal, min_d, max_d = row
    is_ok = (
        total == 22282 and
        minus_one == 1 and
        minus_two == 1 and
        normal == 22280 and
        str(min_d) == "1990-01-01" and
        str(max_d) == "2050-12-31"
    )
    status = "PASS" if is_ok else "FAIL"
    logger.info(
        f"[{status}] dim_data cobertura estrutural: Total={total} | Sentinela -1={minus_one} | "
        f"Sentinela -2={minus_two} | Normais={normal} | Faixa=[{min_d} a {max_d}] ({elapsed:.2f}s)"
    )
    all_passed = is_ok

    # 2. Relacionamento das colunas data_*_sk contra dim_data.data_sk
    date_fks = [
        ("dim_programa.data_disponibilizacao_sk", "gold.dim_programa", "data_disponibilizacao_sk"),
        ("fct_proposta.data_proposta_sk", "gold.fct_proposta", "data_proposta_sk"),
        ("fct_proposta.data_inicio_vigencia_proposta_sk", "gold.fct_proposta", "data_inicio_vigencia_proposta_sk"),
        ("fct_proposta.data_fim_vigencia_proposta_sk", "gold.fct_proposta", "data_fim_vigencia_proposta_sk"),
        ("fct_convenio.data_assinatura_sk", "gold.fct_convenio", "data_assinatura_sk"),
        ("fct_convenio.data_publicacao_sk", "gold.fct_convenio", "data_publicacao_sk"),
        ("fct_convenio.data_inicio_vigencia_sk", "gold.fct_convenio", "data_inicio_vigencia_sk"),
        ("fct_convenio.data_fim_vigencia_sk", "gold.fct_convenio", "data_fim_vigencia_sk"),
        ("fct_convenio.data_limite_prestacao_contas_sk", "gold.fct_convenio", "data_limite_prestacao_contas_sk"),
    ]

    for label, tbl, col in date_fks:
        t0 = time.time()
        query = f"""
            SELECT
                count(*),
                sum(case when d.data_sk is null then 1 else 0 end),
                sum(case when t.{col} = -1 then 1 else 0 end),
                sum(case when t.{col} = -2 then 1 else 0 end)
            FROM {tbl} t
            LEFT JOIN gold.dim_data d ON t.{col} = d.data_sk
        """
        row = run_row_query(cur, query)
        elapsed = time.time() - t0
        total_rows, null_fks, count_null_sentinel, count_oob_sentinel = row
        is_ok = (null_fks == 0)
        status = "PASS" if is_ok else "FAIL"
        if not is_ok:
            all_passed = False
        logger.info(
            f"[{status}] FK Data {label:42}: Total={total_rows:,} | Órfãos={null_fks} | "
            f"Sentinela -1 (NULL)={count_null_sentinel:,} | Sentinela -2 (Fora Janela)={count_oob_sentinel:,} ({elapsed:.2f}s)"
        )

    return all_passed


def reconcile_date_mapping_exact(cur) -> bool:
    logger.info("=" * 70)
    logger.info("5.1. VALIDAÇÃO EXATA DE MAPEAMENTO DE DATAS (SILVER -> GOLD)")
    logger.info("=" * 70)
    all_passed = True

    # 1. dim_programa: data_disponibilizacao -> data_disponibilizacao_sk
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_programa_cadastral s
        JOIN gold.dim_programa g ON s.id_programa = g.id_programa
        WHERE g.data_disponibilizacao_sk != (
            CASE
                WHEN s.data_disponibilizacao IS NULL THEN -1
                WHEN s.data_disponibilizacao < DATE '1990-01-01' OR s.data_disponibilizacao > DATE '2050-12-31' THEN -2
                ELSE CAST(DATE_FORMAT(s.data_disponibilizacao, 'yyyyMMdd') AS INT)
            END
        )
    """)
    prog_diff = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    is_ok = (prog_diff == 0)
    if not is_ok:
        all_passed = False
    logger.info(f"[{'PASS' if is_ok else 'FAIL'}] Exact Date dim_programa.data_disponibilizacao_sk: Divergências={prog_diff} ({elapsed:.2f}s)")

    # 2. fct_proposta: proposta, inicio_vigencia, fim_vigencia
    t0 = time.time()
    cur.execute("""
        SELECT
            SUM(CASE WHEN g.data_proposta_sk != (
                CASE WHEN s.data_proposta IS NULL THEN -1
                     WHEN s.data_proposta < DATE '1990-01-01' OR s.data_proposta > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_proposta, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_inicio_vigencia_proposta_sk != (
                CASE WHEN s.data_inicio_vigencia_proposta IS NULL THEN -1
                     WHEN s.data_inicio_vigencia_proposta < DATE '1990-01-01' OR s.data_inicio_vigencia_proposta > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_inicio_vigencia_proposta, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_fim_vigencia_proposta_sk != (
                CASE WHEN s.data_fim_vigencia_proposta IS NULL THEN -1
                     WHEN s.data_fim_vigencia_proposta < DATE '1990-01-01' OR s.data_fim_vigencia_proposta > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_fim_vigencia_proposta, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END)
        FROM silver.siconv_proposta s
        JOIN gold.fct_proposta g ON s.id_proposta = g.id_proposta
    """)
    prop_diffs = cur.fetchall()[0]
    elapsed = time.time() - t0
    prop_total_diff = sum(prop_diffs)
    is_ok = (prop_total_diff == 0)
    if not is_ok:
        all_passed = False
    logger.info(f"[{'PASS' if is_ok else 'FAIL'}] Exact Date fct_proposta (proposta={prop_diffs[0]}, inicio={prop_diffs[1]}, fim={prop_diffs[2]}): Divergências={prop_total_diff} ({elapsed:.2f}s)")

    # 3. fct_convenio: assinatura, publicacao, inicio_vigencia, fim_vigencia, limite_prestacao_contas
    t0 = time.time()
    cur.execute("""
        WITH canonical AS (
            SELECT DISTINCT
                numero_convenio,
                data_assinatura_convenio,
                data_publicacao_convenio,
                data_inicio_vigencia_convenio,
                data_fim_vigencia_convenio,
                data_limite_prestacao_contas
            FROM silver.siconv_convenio
        )
        SELECT
            SUM(CASE WHEN g.data_assinatura_sk != (
                CASE WHEN s.data_assinatura_convenio IS NULL THEN -1
                     WHEN s.data_assinatura_convenio < DATE '1990-01-01' OR s.data_assinatura_convenio > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_assinatura_convenio, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_publicacao_sk != (
                CASE WHEN s.data_publicacao_convenio IS NULL THEN -1
                     WHEN s.data_publicacao_convenio < DATE '1990-01-01' OR s.data_publicacao_convenio > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_publicacao_convenio, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_inicio_vigencia_sk != (
                CASE WHEN s.data_inicio_vigencia_convenio IS NULL THEN -1
                     WHEN s.data_inicio_vigencia_convenio < DATE '1990-01-01' OR s.data_inicio_vigencia_convenio > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_inicio_vigencia_convenio, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_fim_vigencia_sk != (
                CASE WHEN s.data_fim_vigencia_convenio IS NULL THEN -1
                     WHEN s.data_fim_vigencia_convenio < DATE '1990-01-01' OR s.data_fim_vigencia_convenio > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_fim_vigencia_convenio, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END),
            SUM(CASE WHEN g.data_limite_prestacao_contas_sk != (
                CASE WHEN s.data_limite_prestacao_contas IS NULL THEN -1
                     WHEN s.data_limite_prestacao_contas < DATE '1990-01-01' OR s.data_limite_prestacao_contas > DATE '2050-12-31' THEN -2
                     ELSE CAST(DATE_FORMAT(s.data_limite_prestacao_contas, 'yyyyMMdd') AS INT) END
            ) THEN 1 ELSE 0 END)
        FROM canonical s
        JOIN gold.fct_convenio g ON s.numero_convenio = g.numero_convenio
    """)
    conv_diffs = cur.fetchall()[0]
    elapsed = time.time() - t0
    conv_total_diff = sum(conv_diffs)
    is_ok = (conv_total_diff == 0)
    if not is_ok:
        all_passed = False
    logger.info(f"[{'PASS' if is_ok else 'FAIL'}] Exact Date fct_convenio (assinatura={conv_diffs[0]}, publ={conv_diffs[1]}, inicio={conv_diffs[2]}, fim={conv_diffs[3]}, limite={conv_diffs[4]}): Divergências={conv_total_diff} ({elapsed:.2f}s)")

    return all_passed


def reconcile_orgao_conformance(cur) -> bool:
    logger.info("=" * 70)
    logger.info("6. CONFORMAÇÃO DE ÓRGÃO (GATE R4-A.1)")
    logger.info("=" * 70)

    t0 = time.time()
    cur.execute("""
        WITH sup_prop AS (
            SELECT DISTINCT codigo_orgao_superior AS cd, descricao_orgao_superior AS ds
            FROM silver.siconv_proposta
        ),
        conc_prop AS (
            SELECT DISTINCT codigo_orgao AS cd, descricao_orgao AS ds
            FROM silver.siconv_proposta
        ),
        sup_prog AS (
            SELECT DISTINCT codigo_orgao_superior_programa AS cd, descricao_orgao_superior_programa AS ds
            FROM silver.siconv_programa_cadastral
        ),
        divergences AS (
            SELECT s.cd, 'sup_prop vs conc_prop' as pair FROM sup_prop s JOIN conc_prop c ON s.cd = c.cd WHERE s.ds != c.ds
            UNION ALL
            SELECT s.cd, 'sup_prop vs sup_prog' as pair FROM sup_prop s JOIN sup_prog p ON s.cd = p.cd WHERE s.ds != p.ds
            UNION ALL
            SELECT c.cd, 'conc_prop vs sup_prog' as pair FROM conc_prop c JOIN sup_prog p ON c.cd = p.cd WHERE c.ds != p.ds
        )
        SELECT count(*) FROM divergences
    """)
    diffs = cur.fetchall()[0][0]
    elapsed = time.time() - t0

    is_ok = (diffs == 0)
    status = "PASS" if is_ok else "FAIL"
    logger.info(f"[{status}] Divergências entre papéis institucionais de órgão: {diffs} ({elapsed:.2f}s)")
    return is_ok


def reconcile_convenio_canonical_stability(cur) -> bool:
    logger.info("=" * 70)
    logger.info("7. PROVA DE ESTABILIDADE DA ASSINATURA CANÔNICA DE CONVÊNIO")
    logger.info("=" * 70)

    t0 = time.time()
    cur.execute("""
        WITH canonical_tuples AS (
            SELECT DISTINCT
                numero_convenio, id_proposta, data_assinatura_convenio, data_publicacao_convenio,
                data_inicio_vigencia_convenio, data_fim_vigencia_convenio, data_limite_prestacao_contas,
                situacao_convenio, subsituacao_convenio, situacao_publicacao, situacao_contratacao,
                is_instrumento_ativo, is_opera_obtv, is_assinado, numero_processo, unidade_gestora_emitente,
                quantidade_termos_aditivos, quantidade_prorrogacoes, valor_global_convenio,
                valor_repasse_convenio, valor_contrapartida_convenio, valor_empenhado_convenio,
                valor_desembolsado_convenio, valor_saldo_remanescente_tesouro,
                valor_saldo_remanescente_convenente, valor_rendimento_aplicacao,
                valor_ingresso_contrapartida, valor_global_original_convenio
            FROM silver.siconv_convenio
        )
        SELECT count(*)
        FROM (
            SELECT numero_convenio, count(*) as cnt
            FROM canonical_tuples
            GROUP BY numero_convenio
            HAVING count(*) > 1
        ) t
    """)
    unstable_count = cur.fetchall()[0][0]
    elapsed = time.time() - t0

    is_ok = (unstable_count == 0)
    status = "PASS" if is_ok else "FAIL"
    logger.info(f"[{status}] Convênios com perfis canônicos múltiplos/divergentes: {unstable_count} ({elapsed:.2f}s)")
    return is_ok


def reconcile_financials_proposta(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 70)
    logger.info("8. RECONCILIAÇÃO FINANCEIRA EXATA — PROPOSTAS")
    logger.info("=" * 70)

    t0 = time.time()
    s_row = run_row_query(cur, """
        SELECT
            CAST(coalesce(sum(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2))
        FROM silver.siconv_proposta
    """)
    g_row = run_row_query(cur, """
        SELECT
            CAST(coalesce(sum(CAST(valor_global_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_repasse_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_contrapartida_proposta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2))
        FROM gold.fct_proposta
    """)
    elapsed = time.time() - t0

    metrics = [
        ("Valor Global Proposta", Decimal(str(s_row[0])), Decimal(str(g_row[0])), "valor_global"),
        ("Valor Repasse Proposta", Decimal(str(s_row[1])), Decimal(str(g_row[1])), "valor_repasse"),
        ("Valor Contrapartida Proposta", Decimal(str(s_row[2])), Decimal(str(g_row[2])), "valor_contrapartida"),
    ]

    all_passed = True
    for label, s_val, g_val, base_key in metrics:
        diff = g_val - s_val
        is_ok = (diff == Decimal("0.00"))
        status = "PASS" if is_ok else "FAIL"
        if not is_ok:
            all_passed = False

        baseline_info = ""
        if check_baseline:
            expected_base = HISTORICAL_BASELINE_R4A["financials_proposta"].get(base_key)
            if expected_base is not None:
                matches_base = (g_val == expected_base)
                if not matches_base:
                    all_passed = False
                    status = "FAIL"
                baseline_info = f" | Baseline=R$ {expected_base:,.2f} ({'MATCH' if matches_base else 'MISMATCH'})"

        logger.info(f"[{status}] {label:30}: Silver=R$ {s_val:,.2f} | Gold=R$ {g_val:,.2f} | Diff=R$ {diff:,.2f}{baseline_info}")

    logger.info(f"Reconciliação financeira de propostas finalizada em {elapsed:.2f}s.")
    return all_passed


def reconcile_financials_convenio(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 70)
    logger.info("9. RECONCILIAÇÃO FINANCEIRA EXATA — CONVÊNIOS (CANÔNICA)")
    logger.info("=" * 70)

    t0 = time.time()
    s_row = run_row_query(cur, """
        WITH canonical AS (
            SELECT DISTINCT
                numero_convenio, valor_global_convenio, valor_repasse_convenio,
                valor_contrapartida_convenio, valor_empenhado_convenio, valor_desembolsado_convenio,
                valor_saldo_remanescente_tesouro, valor_saldo_remanescente_convenente,
                valor_rendimento_aplicacao, valor_ingresso_contrapartida, valor_global_original_convenio
            FROM silver.siconv_convenio
        )
        SELECT
            CAST(coalesce(sum(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_contrapartida_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_saldo_remanescente_tesouro AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_saldo_remanescente_convenente AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_rendimento_aplicacao AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_ingresso_contrapartida AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_global_original_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2))
        FROM canonical
    """)
    g_row = run_row_query(cur, """
        SELECT
            CAST(coalesce(sum(CAST(valor_global_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_repasse_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_contrapartida_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_empenhado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_desembolsado_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_saldo_remanescente_tesouro AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_saldo_remanescente_convenente AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_rendimento_aplicacao AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_ingresso_contrapartida AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            CAST(coalesce(sum(CAST(valor_global_original_convenio AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2))
        FROM gold.fct_convenio
    """)
    elapsed = time.time() - t0

    names = [
        ("Valor Global Convênio", "valor_global"),
        ("Valor Repasse Convênio", "valor_repasse"),
        ("Valor Contrapartida Convênio", "valor_contrapartida"),
        ("Valor Empenhado Convênio", "valor_empenhado"),
        ("Valor Desembolsado Convênio", "valor_desembolsado"),
        ("Saldo Remanescente Tesouro", "valor_saldo_remanescente_tesouro"),
        ("Saldo Remanescente Convenente", "valor_saldo_remanescente_convenente"),
        ("Rendimento Aplicação", "valor_rendimento_aplicacao"),
        ("Ingresso Contrapartida", "valor_ingresso_contrapartida"),
        ("Valor Global Original", "valor_global_original"),
    ]

    all_passed = True
    for i, (label, base_key) in enumerate(names):
        s_val = Decimal(str(s_row[i]))
        g_val = Decimal(str(g_row[i]))
        diff = g_val - s_val
        is_ok = (diff == Decimal("0.00"))
        status = "PASS" if is_ok else "FAIL"
        if not is_ok:
            all_passed = False

        baseline_info = ""
        if check_baseline:
            expected_base = HISTORICAL_BASELINE_R4A["financials_convenio_canonical"].get(base_key)
            if expected_base is not None:
                matches_base = (g_val == expected_base)
                if not matches_base:
                    all_passed = False
                    status = "FAIL"
                baseline_info = f" | Baseline=R$ {expected_base:,.2f} ({'MATCH' if matches_base else 'MISMATCH'})"

        logger.info(f"[{status}] {label:32}: Canonical=R$ {s_val:,.2f} | Gold=R$ {g_val:,.2f} | Diff=R$ {diff:,.2f}{baseline_info}")

    logger.info(f"Reconciliação financeira canônica de convênios finalizada em {elapsed:.2f}s.")
    return all_passed


def reconcile_saldo_observations(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 70)
    logger.info("10. FATO OBSERVACIONAL DE SALDO E AUDITORIA DE CONFLITOS")
    logger.info("=" * 70)

    t0 = time.time()
    s_row = run_row_query(cur, """
        SELECT
            count(*),
            count(distinct id_convenio_observacao),
            CAST(coalesce(sum(CAST(valor_saldo_conta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            sum(case when has_source_conflict then 1 else 0 end)
        FROM silver.siconv_convenio
    """)
    g_row = run_row_query(cur, """
        SELECT
            count(*),
            count(distinct id_convenio_observacao),
            CAST(coalesce(sum(CAST(valor_saldo_conta AS DECIMAL(38,2))), 0.00) AS DECIMAL(38,2)),
            sum(case when has_source_conflict then 1 else 0 end)
        FROM gold.fct_convenio_saldo_observacao
    """)
    elapsed = time.time() - t0

    s_cnt, s_dist, s_sum, s_conf = s_row[0], s_row[1], Decimal(str(s_row[2])), s_row[3]
    g_cnt, g_dist, g_sum, g_conf = g_row[0], g_row[1], Decimal(str(g_row[2])), g_row[3]

    diff_cnt = g_cnt - s_cnt
    diff_sum = g_sum - s_sum
    diff_conf = g_conf - s_conf

    is_ok = (diff_cnt == 0 and diff_sum == Decimal("0.00") and diff_conf == 0 and g_cnt == g_dist)
    status = "PASS" if is_ok else "FAIL"

    baseline_info = ""
    if check_baseline:
        expected_checksum = HISTORICAL_BASELINE_R4A["saldo_checksum"]
        matches_base = (g_sum == expected_checksum)
        if not matches_base:
            is_ok = False
            status = "FAIL"
        baseline_info = f" | Baseline Checksum=R$ {expected_checksum:,.2f} ({'MATCH' if matches_base else 'MISMATCH'})"

    logger.info(
        f"[{status}] fct_convenio_saldo_observacao: Total={g_cnt:,} (Silver={s_cnt:,}, Diff={diff_cnt}) | "
        f"PKs Distintas={g_dist:,} | Conflitos={g_conf:,} (Diff={diff_conf}) | "
        f"Checksum Técnico Saldo=R$ {g_sum:,.2f} (Diff=R$ {diff_sum:,.2f}){baseline_info} ({elapsed:.2f}s)"
    )
    return is_ok


def check_bridge_guardrails(cur) -> bool:
    logger.info("=" * 70)
    logger.info("11. GUARDRAILS DE ASSOCIAÇÃO N:N NA BRIDGE")
    logger.info("=" * 70)

    t0 = time.time()
    cur.execute("DESCRIBE gold.bridge_programa_proposta")
    cols = [str(r[0]).strip().lower() for r in cur.fetchall() if r[0] and not str(r[0]).startswith("#")]
    elapsed = time.time() - t0

    prohibited_found = set(cols) & PROHIBITED_BRIDGE_COLUMNS
    is_ok = (len(prohibited_found) == 0)
    status = "PASS" if is_ok else "FAIL"

    logger.info(f"[{status}] Colunas em gold.bridge_programa_proposta: {cols} ({elapsed:.2f}s)")
    if not is_ok:
        logger.error(f"VIOLAÇÃO CRÍTICA DO GUARDRAIL N:N: Colunas proibidas encontradas na bridge: {prohibited_found}")
    else:
        logger.info("[INFO] Guardrail N:N satisfeito: Bridge contém estritamente chaves de relacionamento sem colunas financeiras ou rateios.")

    return is_ok


def main():
    parser = argparse.ArgumentParser(description="Reconciliação dinâmica Silver -> Gold (R4-B).")
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Spark Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Spark Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuário do Thrift Server")
    parser.add_argument("--check-baseline-r4a", action="store_true", help="Checa conformidade com o baseline histórico do snapshot de 16/09/2026")

    args = parser.parse_args()

    mode_str = "HISTÓRICO (BASELINE R4-A)" if args.check_baseline_r4a else "DINÂMICO (SNAPSHOT ATUAL)"
    logger.info("=" * 70)
    logger.info(f"INICIANDO RECONCILIAÇÃO SILVER -> GOLD — MODO {mode_str}")
    logger.info("=" * 70)

    t_global_start = time.time()
    try:
        conn = get_connection(host=args.host, port=args.port, user=args.user)
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Falha ao conectar no Spark Thrift Server ({args.host}:{args.port}): {e}")
        sys.exit(1)

    gates = [
        ("gold_location", lambda: check_gold_location(cur)),
        ("row_counts", lambda: reconcile_row_counts(cur, check_baseline=args.check_baseline_r4a)),
        ("primary_keys", lambda: reconcile_primary_keys(cur)),
        ("referential_integrity", lambda: reconcile_referential_integrity(cur)),
        ("date_mapping", lambda: reconcile_date_mapping(cur)),
        ("date_mapping_exact", lambda: reconcile_date_mapping_exact(cur)),
        ("orgao_conformance", lambda: reconcile_orgao_conformance(cur)),
        ("convenio_canonical_stability", lambda: reconcile_convenio_canonical_stability(cur)),
        ("financials_proposta", lambda: reconcile_financials_proposta(cur, check_baseline=args.check_baseline_r4a)),
        ("financials_convenio", lambda: reconcile_financials_convenio(cur, check_baseline=args.check_baseline_r4a)),
        ("saldo_observations", lambda: reconcile_saldo_observations(cur, check_baseline=args.check_baseline_r4a)),
        ("bridge_guardrails", lambda: check_bridge_guardrails(cur)),
    ]

    results: Dict[str, bool] = {}
    for gate_name, gate_fn in gates:
        try:
            passed = gate_fn()
            results[gate_name] = passed
        except Exception as e:
            logger.error(f"[FAIL] Exceção durante validação do gate '{gate_name}': {e}")
            results[gate_name] = False

    t_global_elapsed = time.time() - t_global_start
    conn.close()

    logger.info("=" * 70)
    logger.info("RESUMO FINAL DA RECONCILIAÇÃO SILVER -> GOLD")
    logger.info("=" * 70)
    all_passed = True
    for gate_name, passed in results.items():
        st = "PASS" if passed else "FAIL"
        if not passed:
            all_passed = False
        logger.info(f"{gate_name:32}: {st}")

    logger.info("-" * 70)
    logger.info(f"Tempo total de execução: {t_global_elapsed:.2f}s")
    if all_passed:
        logger.info("[SUCESSO] Todos os gates de governança e reconciliação da Camada Gold passaram 100%!")
        sys.exit(0)
    else:
        logger.error("[FALHA] Um ou mais gates de reconciliação da Camada Gold falharam!")
        sys.exit(1)


if __name__ == "__main__":
    main()
