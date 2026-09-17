"""
Validação semântica e visual das consultas do Dashboard R5 no Superset.
Testa a execução de consultas dos KPIs, gráficos e tabela detalhada
sob diferentes cenários de filtros:
1. Sem filtros (baseline)
2. Filtro por UF ('MG')
3. Filtro por ano_proposta (2020)
4. Filtro por modalidade ('CONVENIO')
5. Filtro por situacao_convenio ('Em execução')
"""
from superset.app import create_app
app = create_app()

with app.app_context():
    from flask import g
    from superset import db, security_manager
    from superset.connectors.sqla.models import SqlaTable

    admin_user = security_manager.find_user('admin')
    g.user = admin_user

    tbl = db.session.query(SqlaTable).filter_by(
        table_name='vw_superset_proposta_convenio',
        schema='gold'
    ).first()

    if not tbl:
        raise RuntimeError("Dataset vw_superset_proposta_convenio não encontrado!")

    scenarios = [
        ("1. Sem filtros (Baseline Nacional)", []),
        ("2. Filtro por UF = 'MG'", [{'col': 'uf', 'op': '==', 'val': 'MG'}]),
        ("3. Filtro por Ano da Proposta = 2020", [{'col': 'ano_proposta', 'op': '==', 'val': 2020}]),
        ("4. Filtro por Modalidade = 'CONVENIO'", [{'col': 'modalidade', 'op': '==', 'val': 'CONVENIO'}]),
        ("5. Filtro por Situação do Convênio = 'Em execução'", [{'col': 'situacao_convenio', 'op': '==', 'val': 'Em execução'}])
    ]

    print("=" * 80)
    print("VALIDAÇÃO VISUAL / ANALÍTICA DE FILTROS DO DASHBOARD SUPERSET R5")
    print("=" * 80)

    for sc_name, sc_filters in scenarios:
        print(f"\n>>> Cenário: {sc_name}")
        
        # Teste 1: KPIs
        kpi_query = {
            'metrics': [
                'Propostas',
                'Convênios formalizados',
                'Propostas com convênio (%)',
                'Valor global proposto',
                'Valor global conveniado',
                'Valor empenhado',
                'Valor desembolsado'
            ],
            'columns': [],
            'groupby': [],
            'from_dttm': None,
            'to_dttm': None,
            'filter': sc_filters,
            'is_timeseries': False,
            'row_limit': 1
        }
        res_kpi = tbl.query(kpi_query)
        df_kpi = res_kpi.df
        print("  [KPIs Calculados]:")
        for col in df_kpi.columns:
            val = df_kpi[col].iloc[0]
            if isinstance(val, float):
                print(f"    - {col}: {val:,.2f}")
            else:
                print(f"    - {col}: {val}")

        # Teste 2: Gráfico Propostas por ano
        chart_query = {
            'metrics': ['Propostas'],
            'columns': ['ano_proposta'],
            'groupby': ['ano_proposta'],
            'from_dttm': None,
            'to_dttm': None,
            'filter': sc_filters + [{'col': 'ano_proposta', 'op': 'IS NOT NULL', 'val': None}],
            'is_timeseries': False,
            'row_limit': 5,
            'orderby': [('ano_proposta', True)]
        }
        res_chart = tbl.query(chart_query)
        print(f"  [Gráfico Propostas por Ano (Top 5 anos)]: {len(res_chart.df)} linhas retornadas")

        # Teste 3: Tabela Detalhada (Top 3 linhas)
        table_query = {
            'metrics': [],
            'columns': [
                'numero_proposta',
                'nome_proponente',
                'uf',
                'municipio',
                'orgao_concedente',
                'modalidade',
                'situacao_proposta',
                'numero_convenio',
                'situacao_convenio',
                'valor_global_proposta',
                'valor_global_convenio'
            ],
            'groupby': [],
            'from_dttm': None,
            'to_dttm': None,
            'filter': sc_filters,
            'is_timeseries': False,
            'row_limit': 3
        }
        res_tbl = tbl.query(table_query)
        print(f"  [Tabela Detalhada]: {len(res_tbl.df)} amostras obtidas com sucesso.")

    print("\n" + "=" * 80)
    print("TODAS AS CONSULTAS E CENÁRIOS DE FILTROS EXECUTADOS COM SUCESSO SEM ERROS SQL!")
    print("=" * 80)
