#!/usr/bin/env python3
"""
Script de reconciliação Bronze -> Silver (R3-B / R3-B.1).
Valida de forma estrita, somente leitura e distribuída no Spark:
- Contagens de linhas dinâmicas (row counts Bronze vs Silver)
- Unicidade de chaves técnicas, pares compostos e superchaves semânticas
- Comportamento dinâmico de chaves de negócio e conflitos de fonte
- Integridade referencial e auditoria de orfandade de fonte
- Reconciliação financeira exata em DECIMAL(38,2) (tolerância R$ 0,00)
- Preservação da linhagem técnica de metadados

Por padrão opera em modo DINÂMICO (independente do snapshot histórico).
A flag --check-baseline permite checar opcionalmente conformidade com o baseline
do snapshot histórico de 16/09/2026.
"""
import sys
import time
import argparse
import logging
from decimal import Decimal
from typing import Dict, Any, List, Tuple, Set
from pyhive import hive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("reconcile_bronze_silver")

KNOWN_SOURCE_EXCEPTIONS = {321453, 1427146, 296629}

HISTORICAL_BASELINE = {
    "row_counts": {
        "Proposta": 1157619,
        "Ponte Programa-Proposta": 1158975,
        "Programa Elegibilidade": 1257350,
        "Programa Cadastral": 53018,
        "Convênio": 287586,
    },
    "convenio_conflicts": {
        "distinct_convenios": 287584,
        "conflict_keys": 2,
        "conflict_observations": 4,
        "clean_observations": 287582,
    },
    "unlinked_proposals": 78,
    "financials": {
        "Convênio - Valor Global": Decimal("356856636504.87"),
        "Convênio - Valor Repasse": Decimal("331287339337.88"),
        "Convênio - Valor Contrapartida": Decimal("23710083216.75"),
        "Convênio - Valor Empenhado": Decimal("192071916388.96"),
        "Convênio - Valor Desembolsado": Decimal("153212385725.46"),
        "Proposta - Valor Global": Decimal("1495209875334.42"),
        "Proposta - Valor Repasse": Decimal("1425758135735.63"),
        "Proposta - Valor Contrapartida": Decimal("69451779598.79"),
    }
}


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


def reconcile_row_counts(cur, check_baseline: bool = False) -> bool:
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

        baseline_info = ""
        if check_baseline:
            expected_base = HISTORICAL_BASELINE["row_counts"].get(label)
            if expected_base is not None:
                matches_base = (s_cnt == expected_base)
                if not matches_base:
                    all_passed = False
                    status = "FAIL"
                baseline_info = f" | Baseline={expected_base:,} ({'MATCH' if matches_base else 'MISMATCH'})"

        logger.info(f"[{status}] {label}: Bronze={b_cnt:,} | Silver={s_cnt:,} | Diff={diff:,}{baseline_info} ({elapsed:.2f}s)")

    return all_passed


def reconcile_key_uniqueness(cur) -> bool:
    logger.info("=" * 60)
    logger.info("2. UNICIDADE DE CHAVES TÉCNICAS, PARES E SUPERCHAVES")
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

    # 1. Unicidade do par composto na tabela associativa
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

    # 2. Unicidade da superchave semântica de elegibilidade
    t0 = time.time()
    cur.execute("""
        SELECT count(*)
        FROM (
            SELECT
                id_programa,
                modalidade_programa,
                natureza_juridica_programa,
                uf_programa,
                acao_orcamentaria,
                count(*) as cnt
            FROM silver.siconv_programa_elegibilidade
            GROUP BY
                id_programa,
                modalidade_programa,
                natureza_juridica_programa,
                uf_programa,
                acao_orcamentaria
            HAVING count(*) > 1
        ) t
    """)
    superkey_collisions = cur.fetchall()[0][0]
    elapsed = time.time() - t0
    status = "PASS" if superkey_collisions == 0 else "FAIL"
    if superkey_collisions != 0:
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_elegibilidade (business superkey): Colisões={superkey_collisions} ({elapsed:.2f}s)")

    return all_passed


def reconcile_convenio_conflicts(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 60)
    logger.info("3. QUALIDADE E DETECÇÃO DINÂMICA DE CONFLITOS EM CONVÊNIOS")
    logger.info("=" * 60)

    # 1. Agregação dinâmica na Bronze
    t0 = time.time()
    cur.execute("""
        SELECT
            count(distinct NR_CONVENIO) as b_dist_keys,
            sum(case when cnt > 1 then 1 else 0 end) as b_conf_keys,
            sum(case when cnt > 1 then cnt else 0 end) as b_conf_obs,
            sum(case when cnt = 1 then 1 else 0 end) as b_clean_obs
        FROM (
            SELECT NR_CONVENIO, count(*) as cnt
            FROM bronze.siconv_convenio
            GROUP BY NR_CONVENIO
        ) t
    """)
    b_dist_keys, b_conf_keys, b_conf_obs, b_clean_obs = cur.fetchall()[0]

    # 2. Agregação dinâmica na Silver
    cur.execute("""
        SELECT
            count(distinct numero_convenio) as s_dist_keys,
            count(distinct case when source_conflict_count > 1 then numero_convenio else null end) as s_conf_keys,
            sum(case when source_conflict_count > 1 then 1 else 0 end) as s_conf_obs,
            sum(case when has_source_conflict then 1 else 0 end) as s_flag_obs,
            sum(case when not has_source_conflict then 1 else 0 end) as s_clean_obs
        FROM silver.siconv_convenio
    """)
    s_dist_keys, s_conf_keys, s_conf_obs, s_flag_obs, s_clean_obs = cur.fetchall()[0]

    # 3. Verificação de consistência entre flags e contagens
    cur.execute("""
        SELECT count(*)
        FROM silver.siconv_convenio
        WHERE (source_conflict_count > 1 and not has_source_conflict)
           OR (source_conflict_count <= 1 and has_source_conflict)
    """)
    flag_inconsistencies = cur.fetchall()[0][0]
    elapsed = time.time() - t0

    logger.info(f"Convênios distintos (business keys): Bronze={b_dist_keys:,} | Silver={s_dist_keys:,}")
    logger.info(f"Chaves de negócio conflitantes:     Bronze={b_conf_keys:,} | Silver={s_conf_keys:,}")
    logger.info(f"Observações com conflito de fonte:  Bronze={b_conf_obs:,} | Silver={s_conf_obs:,}")
    logger.info(f"Observações flagradas (has_source_conflict=true): {s_flag_obs:,}")
    logger.info(f"Observações limpas (has_source_conflict=false):    Bronze={b_clean_obs:,} | Silver={s_clean_obs:,}")
    logger.info(f"Inconsistências de flag (count vs boolean): {flag_inconsistencies}")

    # Exibe amostra das observações conflitantes
    cur.execute("""
        SELECT numero_convenio, id_convenio_observacao, valor_saldo_conta, source_conflict_count
        FROM silver.siconv_convenio
        WHERE has_source_conflict = true
        ORDER BY numero_convenio, id_convenio_observacao
    """)
    conflict_rows = cur.fetchall()
    logger.info(f"Detalhe das observações conflitantes encontradas ({len(conflict_rows)}):")
    for r in conflict_rows:
        logger.info(f"  NR_CONVENIO={r[0]} | ID_OBS={r[1][:16]}... | SALDO={r[2]} | COUNT={r[3]}")

    dynamic_pass = (
        s_dist_keys == b_dist_keys and
        s_conf_keys == b_conf_keys and
        s_conf_obs == b_conf_obs and
        s_flag_obs == b_conf_obs and
        s_clean_obs == b_clean_obs and
        flag_inconsistencies == 0 and
        len(conflict_rows) == b_conf_obs
    )

    baseline_pass = True
    if check_baseline:
        expected = HISTORICAL_BASELINE["convenio_conflicts"]
        matches = (
            s_dist_keys == expected["distinct_convenios"] and
            s_conf_keys == expected["conflict_keys"] and
            s_conf_obs == expected["conflict_observations"] and
            s_clean_obs == expected["clean_observations"]
        )
        if not matches:
            baseline_pass = False
        logger.info(f"Validação de Baseline Histórico (16/09/2026): {'PASS' if matches else 'FAIL'}")

    passed = dynamic_pass and baseline_pass
    status = "PASS" if passed else "FAIL"
    logger.info(f"[{status}] Reconciliação Dinâmica de Conflitos em Convênios concluída em {elapsed:.2f}s")
    return passed


def reconcile_referential_integrity(cur, check_baseline: bool = False) -> bool:
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

    orphan_set: Set[int] = {int(x) for x in bridge_prop_orphans if x is not None}
    unexpected = orphan_set - KNOWN_SOURCE_EXCEPTIONS
    status = "PASS" if len(unexpected) == 0 else "FAIL"
    if status != "PASS":
        all_passed = False
    logger.info(f"[{status}] silver.siconv_programa_proposta -> silver.siconv_proposta: Órfãos={sorted(list(orphan_set))} | Inesperados={sorted(list(unexpected))} ({elapsed:.2f}s)")

    if check_baseline:
        matches_base = (orphan_set == KNOWN_SOURCE_EXCEPTIONS)
        if not matches_base:
            all_passed = False
            logger.warning(f"[FAIL] Baseline orfandade divergente do histórico: esperado={KNOWN_SOURCE_EXCEPTIONS}, atual={orphan_set}")

    # 5. Propostas sem programa vinculado (métrica puramente informativa INFO)
    t0 = time.time()
    b_unlinked = run_scalar_query(cur, """
        SELECT count(*)
        FROM bronze.siconv_proposta p
        LEFT JOIN bronze.siconv_programa_proposta pp ON p.ID_PROPOSTA = pp.ID_PROPOSTA
        WHERE pp.ID_PROPOSTA IS NULL
    """)
    s_unlinked = run_scalar_query(cur, """
        SELECT count(*)
        FROM silver.siconv_proposta p
        LEFT JOIN silver.siconv_programa_proposta pp ON p.id_proposta = pp.id_proposta
        WHERE pp.id_proposta IS NULL
    """)
    elapsed = time.time() - t0

    diff_unlinked = s_unlinked - b_unlinked
    if diff_unlinked != 0:
        all_passed = False
        logger.error(f"[FAIL] Divergência em propostas sem programa entre Bronze e Silver: Bronze={b_unlinked:,} | Silver={s_unlinked:,}")
    else:
        logger.info(f"[INFO] Propostas legítimas sem vínculo de programa: Bronze={b_unlinked:,} | Silver={s_unlinked:,} (preservadas integralmente, {elapsed:.2f}s)")

    if check_baseline:
        expected_unlinked = HISTORICAL_BASELINE["unlinked_proposals"]
        matches = (s_unlinked == expected_unlinked)
        if not matches:
            all_passed = False
        logger.info(f"[{'PASS' if matches else 'FAIL'}] Baseline propostas sem programa: {s_unlinked} (esperado: {expected_unlinked})")

    return all_passed


def reconcile_financials(cur, check_baseline: bool = False) -> bool:
    logger.info("=" * 60)
    logger.info("5. RECONCILIAÇÃO FINANCEIRA EXATA EM DECIMAL(38,2)")
    logger.info("=" * 60)

    financial_checks = [
        ("Convênio - Valor Global",
         "SELECT sum(cast(replace(VL_GLOBAL_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_global_convenio as decimal(38,2))) FROM silver.siconv_convenio"),

        ("Convênio - Valor Repasse",
         "SELECT sum(cast(replace(VL_REPASSE_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_repasse_convenio as decimal(38,2))) FROM silver.siconv_convenio"),

        ("Convênio - Valor Contrapartida",
         "SELECT sum(cast(replace(VL_CONTRAPARTIDA_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_contrapartida_convenio as decimal(38,2))) FROM silver.siconv_convenio"),

        ("Convênio - Valor Empenhado",
         "SELECT sum(cast(replace(VL_EMPENHADO_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_empenhado_convenio as decimal(38,2))) FROM silver.siconv_convenio"),

        ("Convênio - Valor Desembolsado",
         "SELECT sum(cast(replace(VL_DESEMBOLSADO_CONV, ',', '.') as decimal(38,2))) FROM bronze.siconv_convenio",
         "SELECT sum(cast(valor_desembolsado_convenio as decimal(38,2))) FROM silver.siconv_convenio"),

        ("Proposta - Valor Global",
         "SELECT sum(cast(replace(VL_GLOBAL_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_global_proposta as decimal(38,2))) FROM silver.siconv_proposta"),

        ("Proposta - Valor Repasse",
         "SELECT sum(cast(replace(VL_REPASSE_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_repasse_proposta as decimal(38,2))) FROM silver.siconv_proposta"),

        ("Proposta - Valor Contrapartida",
         "SELECT sum(cast(replace(VL_CONTRAPARTIDA_PROP, ',', '.') as decimal(38,2))) FROM bronze.siconv_proposta",
         "SELECT sum(cast(valor_contrapartida_proposta as decimal(38,2))) FROM silver.siconv_proposta")
    ]

    all_passed = True
    for label, b_sql, s_sql in financial_checks:
        t0 = time.time()
        b_val = run_scalar_query(cur, b_sql)
        s_val = run_scalar_query(cur, s_sql)
        elapsed = time.time() - t0

        b_dec = Decimal(str(b_val)) if b_val is not None else Decimal("0.00")
        s_dec = Decimal(str(s_val)) if s_val is not None else Decimal("0.00")
        diff = s_dec - b_dec

        matches_bronze = (diff == Decimal("0.00"))
        passed = matches_bronze

        baseline_info = ""
        if check_baseline:
            expected_base = HISTORICAL_BASELINE["financials"].get(label)
            if expected_base is not None:
                matches_base = (s_dec == expected_base)
                if not matches_base:
                    passed = False
                baseline_info = f" | Baseline=R$ {expected_base:,.2f} ({'MATCH' if matches_base else 'MISMATCH'})"

        if not passed:
            all_passed = False

        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {label}: Bronze=R$ {b_dec:,.2f} | Silver=R$ {s_dec:,.2f} | Diff=R$ {diff:,.2f}{baseline_info} ({elapsed:.2f}s)")

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
    parser = argparse.ArgumentParser(description="Auditoria e Reconciliação Bronze -> Silver (R3-B / R3-B.1).")
    parser.add_argument("--check-baseline", action="store_true", help="Valida conformidade adicional contra o baseline do snapshot histórico (16/09/2026)")
    parser.add_argument("--host", default="spark-thrift-server", help="Host do Spark Thrift Server")
    parser.add_argument("--port", type=int, default=10000, help="Porta do Spark Thrift Server")
    parser.add_argument("--user", default="airflow", help="Usuário do Spark Thrift Server")

    args = parser.parse_args()

    mode_str = "DINÂMICO + BASELINE HISTÓRICO" if args.check_baseline else "DINÂMICO (Padrão Lakehouse)"
    logger.info(f"Iniciando reconciliação Bronze -> Silver no modo: {mode_str}...")
    start_time = time.time()

    conn = get_connection(host=args.host, port=args.port, user=args.user)
    cur = conn.cursor()

    results = {}
    results["row_counts"] = reconcile_row_counts(cur, check_baseline=args.check_baseline)
    results["key_uniqueness"] = reconcile_key_uniqueness(cur)
    results["convenio_conflicts"] = reconcile_convenio_conflicts(cur, check_baseline=args.check_baseline)
    results["referential_integrity"] = reconcile_referential_integrity(cur, check_baseline=args.check_baseline)
    results["financials"] = reconcile_financials(cur, check_baseline=args.check_baseline)
    results["technical_lineage"] = reconcile_technical_lineage(cur)

    total_elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"RESUMO FINAL DA RECONCILIAÇÃO (Duração total: {total_elapsed:.2f}s)")
    logger.info(f"Modo de execução: {mode_str}")
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
