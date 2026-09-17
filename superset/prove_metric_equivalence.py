"""
Prova rigorosa de equivalência matemática entre métricas com COUNT DISTINCT vs COUNT simples.
Testa equivalência global, agrupada por dimensões e na taxa de formalização.
"""
from pyhive import hive

THRIFT_HOST = "spark-thrift-server"
THRIFT_PORT = 10000
DATABASE = "gold"

def get_connection():
    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT, username="anonymous", database=DATABASE)

def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("PROVA DE EQUIVALÊNCIA MATEMÁTICA — MÉTRICAS R5")
    print("=" * 80)

    # 1. Propostas Global
    print("\n>>> 1. Equivalência Global de Propostas (COUNT(*) vs COUNT(DISTINCT id_proposta)):")
    cur.execute("""
        SELECT
            COUNT(*) AS count_rows,
            COUNT(DISTINCT id_proposta) AS count_distinct_propostas
        FROM gold.vw_superset_proposta_convenio
    """)
    cnt_rows, cnt_dist_prop = cur.fetchone()
    print(f"  COUNT(*):                    {cnt_rows}")
    print(f"  COUNT(DISTINCT id_proposta): {cnt_dist_prop}")
    assert cnt_rows == cnt_dist_prop == 1157619, "Falha na equivalência de propostas!"
    print("  [PASS] count_rows = count_distinct_propostas = 1.157.619")

    # 2. Convênios Global
    print("\n>>> 2. Equivalência Global de Convênios (COUNT(numero_convenio) vs COUNT(DISTINCT numero_convenio)):")
    cur.execute("""
        SELECT
            COUNT(numero_convenio) AS count_convenios,
            COUNT(DISTINCT numero_convenio) AS count_distinct_convenios
        FROM gold.vw_superset_proposta_convenio
    """)
    cnt_conv, cnt_dist_conv = cur.fetchone()
    print(f"  COUNT(numero_convenio):          {cnt_conv}")
    print(f"  COUNT(DISTINCT numero_convenio): {cnt_dist_conv}")
    assert cnt_conv == cnt_dist_conv == 287584, "Falha na equivalência de convênios!"
    print("  [PASS] count_convenios = count_distinct_convenios = 287.584")

    # 3. Prova Agrupada por Dimensões
    dimensions = [
        ("ano_proposta", "uf IS NOT NULL"), # testano por ano_proposta
        ("ano_assinatura", "ano_assinatura IS NOT NULL"),
        ("uf", "uf IS NOT NULL"),
        ("orgao_concedente", "orgao_concedente IS NOT NULL")
    ]

    print("\n>>> 3. Prova Agrupada de Propostas por Dimensões (COUNT(*) vs COUNT(DISTINCT id_proposta)):")
    for dim_col, where_clause in dimensions:
        sql = f"""
        SELECT COUNT(*) FROM (
            SELECT
                {dim_col},
                COUNT(*) AS novo,
                COUNT(DISTINCT id_proposta) AS antigo
            FROM gold.vw_superset_proposta_convenio
            WHERE {where_clause}
            GROUP BY {dim_col}
        ) x WHERE novo != antigo
        """
        cur.execute(sql)
        diff = cur.fetchone()[0]
        print(f"  - Dimensão '{dim_col}': {diff} divergências")
        assert diff == 0, f"Divergência encontrada em {dim_col}!"

    print("\n>>> 4. Prova Agrupada de Convênios por Dimensões (COUNT(numero_convenio) vs COUNT(DISTINCT numero_convenio)):")
    for dim_col, where_clause in dimensions:
        sql = f"""
        SELECT COUNT(*) FROM (
            SELECT
                {dim_col},
                COUNT(numero_convenio) AS novo,
                COUNT(DISTINCT numero_convenio) AS antigo
            FROM gold.vw_superset_proposta_convenio
            WHERE {where_clause}
            GROUP BY {dim_col}
        ) x WHERE novo != antigo
        """
        cur.execute(sql)
        diff = cur.fetchone()[0]
        print(f"  - Dimensão '{dim_col}': {diff} divergências")
        assert diff == 0, f"Divergência encontrada em {dim_col}!"

    # 5. Prova da Taxa de Formalização
    print("\n>>> 5. Prova da Taxa de Formalização (Global e por Dimensões):")
    sql_taxa_global = """
    SELECT
        100.0 * COUNT(DISTINCT CASE WHEN numero_convenio IS NOT NULL THEN id_proposta END) / COUNT(DISTINCT id_proposta) AS taxa_antiga,
        100.0 * COUNT(numero_convenio) / COUNT(*) AS taxa_nova
    FROM gold.vw_superset_proposta_convenio
    """
    cur.execute(sql_taxa_global)
    t_antiga, t_nova = cur.fetchone()
    diff_taxa = abs(float(t_antiga) - float(t_nova))
    print(f"  Taxa Antiga (Global): {t_antiga:.6f}%")
    print(f"  Taxa Nova (Global):   {t_nova:.6f}%")
    print(f"  Diferença:            {diff_taxa:.8f}%")
    assert diff_taxa < 1e-6, "Divergência na taxa global!"
    print("  [PASS] Taxa global equivalente!")

    taxa_dimensions = ["ano_proposta", "uf", "modalidade"]
    for dim_col in taxa_dimensions:
        sql_taxa_dim = f"""
        SELECT COUNT(*) FROM (
            SELECT
                {dim_col},
                100.0 * COUNT(DISTINCT CASE WHEN numero_convenio IS NOT NULL THEN id_proposta END) / COUNT(DISTINCT id_proposta) AS taxa_antiga,
                100.0 * COUNT(numero_convenio) / COUNT(*) AS taxa_nova
            FROM gold.vw_superset_proposta_convenio
            WHERE {dim_col} IS NOT NULL
            GROUP BY {dim_col}
        ) x WHERE ABS(taxa_antiga - taxa_nova) > 0.00001
        """
        cur.execute(sql_taxa_dim)
        diff = cur.fetchone()[0]
        print(f"  - Taxa agrupada por '{dim_col}': {diff} divergências")
        assert diff == 0, f"Divergência na taxa por {dim_col}!"

    print("\n" + "=" * 80)
    print("TODAS AS PROVAS DE EQUIVALÊNCIA FORAM APROVADAS COM 0 DIVERGÊNCIAS!")
    print("=" * 80)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
