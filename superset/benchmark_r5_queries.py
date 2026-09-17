"""
Benchmark reproduzível de consultas analíticas do Superset R5 contra o Spark Thrift Server.
Mede primeira execução e segunda execução consecutiva para Q1 a Q6.
Gera relatórios de tempo e captura EXPLAIN das consultas de agregação.
"""
import time
from typing import Dict, Any, List
from pyhive import hive

THRIFT_HOST = "spark-thrift-server"
THRIFT_PORT = 10000
DATABASE = "gold"

QUERIES = {
    "Q1 — KPI Propostas": (
        "SELECT COUNT(DISTINCT id_proposta) FROM gold.vw_superset_proposta_convenio"
    ),
    "Q2 — KPI Convênios": (
        "SELECT COUNT(DISTINCT numero_convenio) FROM gold.vw_superset_proposta_convenio"
    ),
    "Q3 — Propostas por ano": """
SELECT
    ano_proposta,
    COUNT(DISTINCT id_proposta) AS propostas
FROM gold.vw_superset_proposta_convenio
WHERE ano_proposta IS NOT NULL
GROUP BY ano_proposta
ORDER BY ano_proposta ASC
""",
    "Q4 — Convênios por ano": """
SELECT
    ano_assinatura,
    COUNT(DISTINCT numero_convenio) AS convenios
FROM gold.vw_superset_proposta_convenio
WHERE ano_assinatura IS NOT NULL
GROUP BY ano_assinatura
ORDER BY ano_assinatura ASC
""",
    "Q5 — Propostas por UF": """
SELECT
    uf,
    COUNT(DISTINCT id_proposta) AS propostas
FROM gold.vw_superset_proposta_convenio
WHERE uf IS NOT NULL
GROUP BY uf
ORDER BY propostas DESC
""",
    "Q6 — Convênios por órgão": """
SELECT
    orgao_concedente,
    COUNT(DISTINCT numero_convenio) AS convenios
FROM gold.vw_superset_proposta_convenio
WHERE orgao_concedente IS NOT NULL
GROUP BY orgao_concedente
ORDER BY convenios DESC
LIMIT 10
"""
}

def get_connection():
    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, username="anonymous", database=DATABASE)

def run_query(conn, sql: str):
    cursor = conn.cursor()
    t0 = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    t1 = time.perf_counter()
    cursor.close()
    return t1 - t0, rows

def main():
    print("=" * 80)
    print("BENCHMARK INICIAL R5-MVP — CONSULTAS ATUAIS (VIEW + COUNT DISTINCT)")
    print(f"Endpoint: {THRIFT_HOST}:{THRIFT_PORT}/{DATABASE}")
    print("=" * 80)

    conn = get_connection()

    # Verificar configurações Spark Thrift
    cur = conn.cursor()
    for conf in ["spark.sql.shuffle.partitions", "spark.sql.autoBroadcastJoinThreshold"]:
        try:
            cur.execute(f"SET {conf}")
            res = cur.fetchall()
            print(f"Config: {res}")
        except Exception as e:
            print(f"Config {conf}: erro {e}")
    cur.close()

    results = []

    for q_name, sql in QUERIES.items():
        print(f"\n>>> Executando: {q_name} ...")
        
        # Run 1: Primeira execução
        print("  - Execução 1 iniciando...")
        t1, rows1 = run_query(conn, sql)
        print(f"    Concluído em {t1:.2f} s | Linhas: {len(rows1)} | Amostra: {rows1[:2]}")

        # Run 2: Segunda execução consecutiva
        print("  - Execução 2 iniciando...")
        t2, rows2 = run_query(conn, sql)
        print(f"    Concluído em {t2:.2f} s | Linhas: {len(rows2)}")

        status_sla = "PASS (<60s)" if max(t1, t2) < 60 else ("WARN (run1>=60s)" if t2 < 60 else "FAIL (>=60s)")

        results.append({
            "query": q_name,
            "t1": t1,
            "t2": t2,
            "rows": len(rows1),
            "status": status_sla,
            "over_60": (t1 >= 60 or t2 >= 60)
        })

    print("\n" + "=" * 80)
    print("TABELA RESUMO DO DIAGNÓSTICO INICIAL:")
    print(f"{'Query':<30} | {'Exec 1 (s)':<10} | {'Exec 2 (s)':<10} | {'Linhas':<6} | {'>60 s?':<6} | {'Status SLA'}")
    print("-" * 80)
    for r in results:
        print(f"{r['query']:<30} | {r['t1']:<10.2f} | {r['t2']:<10.2f} | {r['rows']:<6} | {'SIM' if r['over_60'] else 'NAO':<6} | {r['status']}")
    print("=" * 80)

    # Coletar EXPLAIN FORMATTED para Q3, Q5, Q6
    print("\n" + "=" * 80)
    print("CAPTURA DE EXPLAIN FORMATTED (Q3, Q5, Q6)")
    print("=" * 80)
    for q_key in ["Q3 — Propostas por ano", "Q5 — Propostas por UF", "Q6 — Convênios por órgão"]:
        sql = QUERIES[q_key]
        print(f"\n--- EXPLAIN FORMATTED: {q_key} ---")
        cur = conn.cursor()
        try:
            cur.execute(f"EXPLAIN FORMATTED {sql}")
            exp_rows = cur.fetchall()
            full_explain = "\n".join([r[0] for r in exp_rows])
            # Imprime primeiras 40 linhas
            preview = "\n".join(full_explain.split("\n")[:40])
            print(preview)
            print("... [truncated preview]")
        except Exception as e:
            print(f"Erro no explain de {q_key}: {e}")
        cur.close()

    conn.close()

if __name__ == "__main__":
    main()
