"""
Benchmark das consultas Q1 a Q6 na VIEW com MÉTRICAS SIMPLIFICADAS (sem COUNT DISTINCT).
"""
import time
from pyhive import hive

THRIFT_HOST = "spark-thrift-server"
THRIFT_PORT = 10000
DATABASE = "gold"

QUERIES = {
    "Q1 — KPI Propostas (COUNT(*))": (
        "SELECT COUNT(*) FROM gold.vw_superset_proposta_convenio"
    ),
    "Q2 — KPI Convênios (COUNT(numero_convenio))": (
        "SELECT COUNT(numero_convenio) FROM gold.vw_superset_proposta_convenio"
    ),
    "Q3 — Propostas por ano (COUNT(*))": """
SELECT
    ano_proposta,
    COUNT(*) AS propostas
FROM gold.vw_superset_proposta_convenio
WHERE ano_proposta IS NOT NULL
GROUP BY ano_proposta
ORDER BY ano_proposta ASC
""",
    "Q4 — Convênios por ano (COUNT(num))": """
SELECT
    ano_assinatura,
    COUNT(numero_convenio) AS convenios
FROM gold.vw_superset_proposta_convenio
WHERE ano_assinatura IS NOT NULL
GROUP BY ano_assinatura
ORDER BY ano_assinatura ASC
""",
    "Q5 — Propostas por UF (COUNT(*))": """
SELECT
    uf,
    COUNT(*) AS propostas
FROM gold.vw_superset_proposta_convenio
WHERE uf IS NOT NULL
GROUP BY uf
ORDER BY propostas DESC
""",
    "Q6 — Convênios por órgão (COUNT(num))": """
SELECT
    orgao_concedente,
    COUNT(numero_convenio) AS convenios
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
    print("BENCHMARK — VIEW COM MÉTRICAS SIMPLIFICADAS (COUNT SIMPLES)")
    print("=" * 80)
    conn = get_connection()
    results = []

    for q_name, sql in QUERIES.items():
        print(f"\n>>> Executando: {q_name} ...")
        t1, rows1 = run_query(conn, sql)
        print(f"  - Execução 1: {t1:.2f} s | Linhas: {len(rows1)} | Amostra: {rows1[:2]}")
        t2, rows2 = run_query(conn, sql)
        print(f"  - Execução 2: {t2:.2f} s | Linhas: {len(rows2)}")

        results.append({
            "query": q_name,
            "t1": t1,
            "t2": t2,
            "rows": len(rows1)
        })

    print("\n" + "=" * 80)
    print("TABELA RESUMO — VIEW + MÉTRICAS SIMPLIFICADAS:")
    print(f"{'Query':<38} | {'Exec 1 (s)':<10} | {'Exec 2 (s)':<10} | {'Linhas':<6}")
    print("-" * 80)
    for r in results:
        print(f"{r['query']:<38} | {r['t1']:<10.2f} | {r['t2']:<10.2f} | {r['rows']:<6}")
    print("=" * 80)
    conn.close()

if __name__ == "__main__":
    main()
