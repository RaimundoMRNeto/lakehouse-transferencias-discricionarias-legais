#!/usr/bin/env python3
"""
Script de reconciliação Bronze -> Silver (R3-B).
Valida de forma estrita, somente leitura e distribuída no Spark:
- Contagens de linhas (row counts)
- Unicidade de chaves técnicas e pares compostos
- Comportamento de chaves de negócio e conflitos legítimos
- Integridade referencial e catálogo de orfandades conhecidas
- Reconciliação financeira exata com DECIMAL(38,2)
- Preservação da linhagem técnica de metadados
"""
import sys
import time
import logging
from decimal import Decimal
from typing import Dict, Any, List, Tuple
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("reconcile_bronze_silver")

KNOWN_ORPHAN_PROPOSALS = {321453, 1427146, 296629}


def get_connection(host: str = "spark-thrift-server", port: int = 10000, user: str = "airflow"):
    return hive.Connection(host=host, port=port, username=user)


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


def reconcile_row_counts(cur) -> bool:
    logger.info("=" * 60)
    logger.info("1. RECONCILIAÇÃO DE CONTAGEM DE LINHAS (ROW COUNTS)")
    logger.info("=" * 60)

    tables = [
        ("Proposta", "bronze.siconv_proposta", "silver.siconv_proposta", "count(*)", "count(*)"),
        ("Ponte Programa-Proposta", "bronze.siconv_programa_proposta", "silver.siconv_programa_proposta", "count(*)", "count(*)"),
        ("Programa Elegibilidade", "bronze.siconv_programa", "silver.siconv_programa_elegibilidade", "count(*)", "count(*)"),
        ("Programa Cadastral", "bronze.siconv_programa", "silver.siconv_programa_cadastral", "count(distinct ID_PROGRAMA)", "count(*)"),
        ("Convênio", "bronze.siconv_convenio", "silver.siconv_convenio", "count(*)", "count(*)")
    ]

    all_passed = True
    for label, b_tbl, s_tbl, b_expr, s_expr in tables:
        t0 = time.time()
        b_cnt = run_scalar_query(cur, f"SELECT {b_expr} FROM {b_tbl}")
        s_cnt = run_scalar_query(cur, f"SELECT {s_expr} FROM {s_tbl}")
        elapsed = time.time() - t0

        diff = s_cnt - b_cnt
        status = "PASS" if diff == 0 else "FAIL"
        if diff != 0:
            all_passed = False

        logger.info(f"[{status}] {label}: Bronze={b_cnt:,} | Silver={s_cnt:,} | Diff={diff:,} ({elapsed:.2f}s)")

    return all_passed


def reconcile_key_uniqueness(cur) -> bool:
    logger.info("=" * 60)
    logger.info("2. UNICIDADE DE CHAVES TÉCNICAS E PARES COMPOSTOS")
    logger.info("=" * 60)

    checks = [
        ("silver.siconv_proposta", "id_proposta", "count(*)", "count(distinct id_proposta)"),
        ("silver.siconv_programa_cadastral", "id_programa", "count(*)", "count(distinct id_programa)"),
        ("silver.siconv_programa_elegibilidade", "id_programa_elegibilidade", "count(*)", "count(distinct id_programa_elegibilidade)"),
        ("silver.siconv_convenio", "id_convenio_observacao", "count(*)", "count(distinct id_convenio_observacao)")
    ]

    all_passed = True
    for tbl, col, cnt_expr, dist_expr in checks:
        t0 = time.time()
        row = run_row_query(cur, f"SELECT {cnt_expr}, {dist_expr} FROM {tbl}")
        elapsed = time.time() - t0
        total, distinct = row[0], row[1]
        diff = total - distinct
        status = "PASS" if diff == 0 else "FAIL"
        if diff != 0:
            all_passed = False
        logger.info(f"[{status}] {tbl} ({col}): Total={total:,} | Distintos={distinct:,} | Colisões={diff} ({elapsed:.2f}s)")

    # Test pair uniqueness on bridge
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM (
            SELECT id_programa, id_proposta, count(*) as cnt
            FROM silver.siconv_programa_proposta
            GROUP BY id_programa, id_proposta
            HAVING count(*) > 1
        ) t
    """)
    bridge_collisions = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if bridge_collisions == 0 else "FAIL"
    if bridge_collisions != 0:
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_proposta (id_programa, id_proposta): Colisões={bridge_collisions} ({elapsed:.2f}s)")

    return all_passed


def reconcile_convenio_conflicts(cur) -> bool:
    logger.info("=" * 60)
    logger.info("3. QUALIDADE E DETECÇÃO DOS CONFLITOS DE CONVÊNIO")
    logger.info("=" * 60)

    t0 = time.time()
    cur.execute("""
        SELECT
            count(distinct numero_convenio) as distinct_convenios,
            sum(case when source_conflict_count > 1 then 1 else 0 end) as conflict_observations,
            count(distinct case when source_conflict_count > 1 then numero_convenio else null end) as conflict_keys,
            sum(case when has_source_conflict then 1 else 0 end) as flagged_observations,
            sum(case when not has_source_conflict then 1 else 0 end) as clean_observations
        FROM silver.siconv_convenio
    """)
    row = cur.fetchall()[0]
    elapsed = time.time() - t0

    dist_keys, conf_obs, conf_keys, flag_obs, clean_obs = row

    logger.info(f"Convênios distintos (business keys): {dist_keys:,} (esperado: 287.584)")
    logger.info(f"Chaves de negócio conflitantes: {conf_keys} (esperado: 2)")
    logger.info(f"Observações com conflito de fonte: {conf_obs} (esperado: 4)")
    logger.info(f"Observações marcadas has_source_conflict=true: {flag_obs} (esperado: 4)")
    logger.info(f"Observações sem conflito has_source_conflict=false: {clean_obs} (esperado: 287.582)")

    # Detalhar as chaves conflitantes
    cur.execute("""
        SELECT numero_convenio, id_convenio_observacao, valor_saldo_conta, source_conflict_count
        FROM silver.siconv_convenio
        WHERE has_source_conflict = true
        ORDER BY numero_convenio, id_convenio_observacao
    """)
    conflict_rows = cur.fetchall()
    logger.info("Detalhe das observações conflitantes:")
    for r in conflict_rows:
        logger.info(f"  NR_CONVENIO={r[0]} | ID_OBS={r[1][:16]}... | SALDO={r[2]} | COUNT={r[3]}")

    passed = (
        dist_keys == 287584 and
        conf_keys == 2 and
        conf_obs == 4 and
        flag_obs == 4 and
        clean_obs == 287582 and
        len(conflict_rows) == 4
    )
    status = "PASS" if passed else "FAIL"
    logger.info(f"[{status}] Validação de Conflitos de Convênio concluída em {elapsed:.2f}s")
    return passed


def reconcile_referential_integrity(cur) -> bool:
    logger.info("=" * 60)
    logger.info("4. INTEGRIDADE REFERENCIAL E AUDITORIA DE ORFANDADE")
    logger.info("=" * 60)

    all_passed = True

    # 1. Convênio -> Proposta
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_convenio c
        LEFT JOIN silver.siconv_proposta p ON c.id_proposta = p.id_proposta
        WHERE p.id_proposta IS NULL
    """)
    conv_orphans = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if conv_orphans == 0 else "FAIL"
    if conv_orphans != 0:
        all_passed = False
    logger.info(f"[{status}] silver.siconv_convenio -> silver.siconv_proposta: Órfãos={conv_orphans} ({elapsed:.2f}s)")

    # 2. Programa Elegibilidade -> Programa Cadastral
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_programa_elegibilidade e
        LEFT JOIN silver.siconv_programa_cadastral c ON e.id_programa = c.id_programa
        WHERE c.id_programa IS NULL
    """)
    eleg_orphans = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if eleg_orphans == 0 else "FAIL"
    if eleg_orphans != 0:
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_elegibilidade -> silver.siconv_programa_cadastral: Órfãos={eleg_orphans} ({elapsed:.2f}s)")

    # 3. Ponte -> Programa Cadastral
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_programa_proposta pp
        LEFT JOIN silver.siconv_programa_cadastral c ON pp.id_programa = c.id_programa
        WHERE c.id_programa IS NULL
    """)
    bridge_prog_orphans = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if bridge_prog_orphans == 0 else "FAIL"
    if bridge_prog_orphans != 0:
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_proposta -> silver.siconv_programa_cadastral: Órfãos={bridge_prog_orphans} ({elapsed:.2f}s)")

    # 4. Ponte -> Proposta (órfãos conhecidos vs inesperados)
    t0 = time.time()
    cur.execute("""
        SELECT distinct pp.id_proposta
        FROM silver.siconv_programa_proposta pp
        LEFT JOIN silver.siconv_proposta p ON pp.id_proposta = p.id_proposta
        WHERE p.id_proposta IS NULL
    """)
    bridge_prop_orphans = [r[0] for r in cur.fetchall()]
    elapsed = time.time() - t0

    orphan_set = set(bridge_prop_orphans)
    unexpected = orphan_set - KNOWN_ORPHAN_PROPOSALS
    status = "PASS" if len(unexpected) == 0 and len(orphan_set) <= 3 else "FAIL"
    if status != "PASS":
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_proposta -> silver.siconv_proposta: Órfãos={sorted(list(orphan_set))} | Inesperados={sorted(list(unexpected))} ({elapsed:.2f}s)")

    # 5. Propostas sem programa vinculado (esperado: 78)
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_proposta p
        LEFT JOIN silver.siconv_programa_proposta pp ON p.id_proposta = pp.id_proposta
        WHERE pp.id_proposta IS NULL
    """)
    unlinked_proposals = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if unlinked_proposals == 78 else "WARN"
    logger.info(f"[{status}] Propostas legítimas sem vínculo de programa: {unlinked_proposals} (esperado: 78) ({elapsed:.2f}s)")

    return all_passed


def reconcile_financials(cur) -> bool:
    logger.info("=" * 60)
    logger.info("5. RECONCILIAÇÃO FINANCEIRA EXATA EM DECIMAL(38,2)")
    logger.info("=" * 60)

    financial_checks = [
        ("Convênio - Valor Global",
         "SELECT sum(cast(replace(VL_GLOBAL_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_global_convenio as decimal(38,2))) FROM silver.siconv_convenio",
         Decimal("356856636504.87")),

        ("Convênio - Valor Repasse",
         "SELECT sum(cast(replace(VL_REPASSE_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_repasse_convenio as decimal(38,2))) FROM silver.siconv_convenio",
         Decimal("331287339337.88")),

        ("Convênio - Valor Contrapartida",
         "SELECT sum(cast(replace(VL_CONTRAPARTIDA_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_contrapartida_convenio as decimal(38,2))) FROM silver.siconv_convenio",
         Decimal("23710083216.75")),

        ("Convênio - Valor Empenhado",
         "SELECT sum(cast(replace(VL_EMPENHADO_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_empenhado_convenio as decimal(38,2))) FROM silver.siconv_convenio",
         Decimal("192071916388.96")),

        ("Convênio - Valor Desembolsado",
         "SELECT sum(cast(replace(VL_DESEMBOLSADO_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_desembolsado_convenio as decimal(38,2))) FROM silver.siconv_convenio",
         Decimal("153212385725.46")),

        ("Proposta - Valor Global",
         "SELECT sum(cast(replace(VL_GLOBAL_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_global_proposta as decimal(38,2))) FROM silver.siconv_proposta",
         Decimal("1495209875334.42")),

        ("Proposta - Valor Repasse",
         "SELECT sum(cast(replace(VL_REPASSE_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_repasse_proposta as decimal(38,2))) FROM silver.siconv_proposta",
         Decimal("1425758135735.63")),

        ("Proposta - Valor Contrapartida",
         "SELECT sum(cast(replace(VL_CONTRAPARTIDA_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_contrapartida_proposta as decimal(38,2))) FROM silver.siconv_proposta",
         Decimal("69451779598.79"))
    ]

    all_passed = True
    for label, b_sql, s_sql, expected_val in financial_checks:
        t0 = time.time()
        b_val = run_scalar_query(cur, b_sql)
        s_val = run_scalar_query(cur, s_sql)
        elapsed = time.time() - t0

        b_dec = Decimal(str(b_val)) if b_val is not None else Decimal("0.00")
        s_dec = Decimal(str(s_val)) if s_val is not None else Decimal("0.00")
        diff = s_dec - b_dec

        matches_expected = (s_dec == expected_val)
        matches_bronze = (diff == Decimal("0.00"))
        passed = matches_expected and matches_bronze

        if not passed:
            all_passed = False

        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {label}: Bronze=R$ {b_dec:,.2f} | Silver=R$ {s_dec:,.2f} | Diff=R$ {diff:,.2f} ({elapsed:.2f}s)")

    return all_passed


def reconcile_technical_lineage(cur) -> bool:
    logger.info("=" * 60)
    logger.info("6. PRESERVAÇÃO DA LINHAGEM TÉCNICA DE METADADOS")
    logger.info("=" * 60)

    tables = [
        "silver.siconv_proposta",
        "silver.siconv_programa_proposta",
        "silver.siconv_programa_cadastral",
        "silver.siconv_programa_elegibilidade",
        "silver.siconv_convenio"
    ]

    all_passed = True
    for tbl in tables:
        t0 = time.time()
        cur.execute(f"""
            SELECT
                count(*) as total,
                sum(case when __ingested_at_utc is null then 1 else 0 end) as null_ts,
                sum(case when __ingestion_run_id is null then 1 else 0 end) as null_run,
                sum(case when __source_file is null then 1 else 0 end) as null_file,
                sum(case when __source_sha256 is null then 1 else 0 end) as null_sha
            FROM {tbl}
        """)
        row = cur.fetchall()[0]
        elapsed = time.time() - t0
        total, null_ts, null_run, null_file, null_sha = row

        passed = (null_ts == 0 and null_run == 0 and null_file == 0 and null_sha == 0)
        if not passed:
            all_passed = False
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {tbl}: Total={total:,} | Nulos(ts={null_ts}, run={null_run}, file={null_file}, sha={null_sha}) ({elapsed:.2f}s)")

    return all_passed


def main():
    logger.info("Iniciando auditoria completa e reconciliação Bronze -> Silver (R3-B)...")
    start_time = time.time()

    conn = get_connection()
    cur = conn.cursor()

    results = {}
    results["row_counts"] = reconcile_row_counts(cur)
    results["key_uniqueness"] = reconcile_key_uniqueness(cur)
    results["convenio_conflicts"] = reconcile_convenio_conflicts(cur)
    results["referential_integrity"] = reconcile_referential_integrity(cur)
    results["financials"] = reconcile_financials(cur)
    results["technical_lineage"] = reconcile_technical_lineage(cur)

    total_elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"RESUMO FINAL DA RECONCILIAÇÃO (Duração total: {total_elapsed:.2f}s)")
    logger.info("=" * 60)

    overall_pass = True
    for section, passed in results.items():
        st = "PASS" if passed else "FAIL"
        logger.info(f"  * {section}: {st}")
        if not passed:
            overall_pass = False

    if overall_pass:
        logger.info("\n>>> TODOS OS GATES DE RECONCILIAÇÃO FORAM APROVADOS COM SUCESSO (0 FALHAS) <<<\n")
        sys.exit(0)
    else:
        logger.error("\n>>> FALHA NA RECONCILIAÇÃO BRONZE -> SILVER <<<\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
