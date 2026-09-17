"""
Simulação de Carga Concorrente no Apache Superset.
Dispara as 12 consultas do Dashboard Executivo R5 simultaneamente via ThreadPoolExecutor.
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyhive import hive

THRIFT_HOST = "spark-thrift-server"
THRIFT_PORT = 10000
DATABASE = "gold"

QUERIES = {
    "KPI 1 — Propostas": "SELECT COUNT(*) FROM gold.mart_superset_proposta_convenio",
    "KPI 2 — Convênios": "SELECT COUNT(numero_convenio) FROM gold.mart_superset_proposta_convenio",
    "KPI 3 — Taxa Formalização": """
SELECT
    CASE WHEN COUNT(*) = 0 THEN NULL
         ELSE 100.0 * COUNT(numero_convenio) / COUNT(*)
    END
FROM gold.mart_superset_proposta_convenio
""",
    "KPI 4 — Valor Proposto": "SELECT SUM(valor_global_proposta) FROM gold.mart_superset_proposta_convenio",
    "KPI 5 — Valor Conveniado": "SELECT SUM(valor_global_convenio) FROM gold.mart_superset_proposta_convenio",
    "KPI 6 — Valor Empenhado": "SELECT SUM(valor_empenhado_convenio) FROM gold.mart_superset_proposta_convenio",
    "KPI 7 — Valor Desembolsado": "SELECT SUM(valor_desembolsado_convenio) FROM gold.mart_superset_proposta_convenio",
    "Chart 1 — Propostas por Ano": """
SELECT ano_proposta, COUNT(*) AS propostas
FROM gold.mart_superset_proposta_convenio
WHERE ano_proposta IS NOT NULL
GROUP BY ano_proposta ORDER BY ano_proposta ASC
""",
    "Chart 2 — Convênios por Ano": """
SELECT ano_assinatura, COUNT(numero_convenio) AS convenios
FROM gold.mart_superset_proposta_convenio
WHERE ano_assinatura IS NOT NULL
GROUP BY ano_assinatura ORDER BY ano_assinatura ASC
""",
    "Chart 3 — Propostas por UF": """
SELECT uf, COUNT(*) AS propostas
FROM gold.mart_superset_proposta_convenio
WHERE uf IS NOT NULL
GROUP BY uf ORDER BY propostas DESC
""",
    "Chart 4 — Convênios por Órgão": """
SELECT orgao_concedente, COUNT(numero_convenio) AS convenios
FROM gold.mart_superset_proposta_convenio
WHERE orgao_concedente IS NOT NULL
GROUP BY orgao_concedente ORDER BY convenios DESC LIMIT 10
""",
    "Table — Detalhamento": """
SELECT id_proposta, numero_proposta, ano_proposta, nome_proponente, uf,
       municipio, orgao_concedente, situacao_proposta, valor_global_proposta,
       tem_convenio, numero_convenio, situacao_convenio, valor_global_convenio
FROM gold.mart_superset_proposta_convenio
ORDER BY valor_global_proposta DESC
LIMIT 15
"""
}

def execute_slice(name, sql):
    conn = hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, username="anonymous", database=DATABASE)
    cursor = conn.cursor()
    t0 = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    t1 = time.perf_counter()
    cursor.close()
    conn.close()
    return name, t1 - t0, len(rows)

def main():
    print("=" * 80)
    print("TESTE DE CONCORRÊNCIA — 12 SLICES DO DASHBOARD SIMULTÂNEOS (MART FÍSICO)")
    print("=" * 80)

    t_start = time.perf_counter()
    results = []

    # Superset envia requisições em paralelo do browser (típico 4 a 6 workers)
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(execute_slice, name, sql) for name, sql in QUERIES.items()]
        for fut in as_completed(futures):
            name, duration, row_count = fut.result()
            print(f"  [OK] {name:<32} executou em {duration:5.2f} s ({row_count} linhas)")
            results.append((name, duration, row_count))

    t_total = time.perf_counter() - t_start

    print("-" * 80)
    print(f"TEMPO TOTAL PARA CARREGAR TODOS OS 12 COMPONENTES: {t_total:.2f} s")
    print(f"MAIOR TEMPO INDIVIDUAL: {max(r[1] for r in results):.2f} s")
    print(f"MENOR TEMPO INDIVIDUAL: {min(r[1] for r in results):.2f} s")
    print(f"MÉDIA POR COMPONENTE:   {sum(r[1] for r in results)/len(results):.2f} s")
    print("=" * 80)

if __name__ == "__main__":
    main()
