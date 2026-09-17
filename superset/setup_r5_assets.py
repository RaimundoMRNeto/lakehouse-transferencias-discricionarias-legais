"""
Setup e exportação de assets do Superset para o R5-MVP.
Garante a conectividade com o Spark Thrift Server, cria o dataset,
adiciona as 7 métricas oficiais, cria os 12 slices (7 KPIs, 4 gráficos, 1 tabela),
monta o dashboard executivo com layout estruturado e 8 filtros nativos,
e exporta os assets para superset/assets/r5_mvp/.
"""
import os
import json
import uuid
from typing import Dict, Any, List
from superset.app import create_app

app = create_app()

with app.app_context():
    from flask import g
    from superset import db, security_manager
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable, TableColumn, SqlMetric
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    from superset.commands.dashboard.export import ExportDashboardsCommand
    from sqlalchemy import text

    # 1. Autenticação interna de serviço
    admin_user = security_manager.find_user('admin')
    if not admin_user:
        raise RuntimeError("Usuário 'admin' não encontrado no Superset!")
    g.user = admin_user

    print(f"[1/6] Usuário de serviço configurado: {admin_user.username}")

    # 2. Conexão de Banco com o Spark Thrift Server
    db_name = "Spark Thrift Server"
    sqlalchemy_uri = "hive://spark-thrift-server:10000/gold"

    database = db.session.query(Database).filter_by(database_name=db_name).first()
    if not database:
        database = Database(
            database_name=db_name,
            sqlalchemy_uri=sqlalchemy_uri,
            expose_in_sqllab=True,
            allow_run_async=False,
            allow_ctas=False,
            allow_cvas=False,
            allow_dml=False
        )
        db.session.add(database)
        db.session.commit()
        print(f"[2/6] Conexão '{db_name}' criada com sucesso.")
    else:
        database.sqlalchemy_uri = sqlalchemy_uri
        db.session.commit()
        print(f"[2/6] Conexão '{db_name}' já existente validada.")

    # Validação do Gate de conectividade
    with database.get_sqla_engine() as engine:
        with engine.connect() as conn:
            cnt = conn.execute(text("SELECT COUNT(*) FROM gold.vw_superset_proposta_convenio")).scalar()
            print(f"[2/6] Gate de conectividade aprovado: {cnt:,} linhas na view.")

    # 3. Dataset da View Semântica
    table_name = "vw_superset_proposta_convenio"
    schema_name = "gold"

    tbl = db.session.query(SqlaTable).filter_by(
        database_id=database.id,
        table_name=table_name,
        schema=schema_name
    ).first()

    if not tbl:
        tbl = SqlaTable(
            table_name=table_name,
            schema=schema_name,
            database=database,
            description="Transferências — Propostas e Convênios (View Semântica Gold)"
        )
        db.session.add(tbl)
        db.session.commit()
        print(f"[3/6] Dataset '{table_name}' registrado.")
    else:
        print(f"[3/6] Dataset '{table_name}' já existente validado.")

    tbl.fetch_metadata()
    db.session.commit()

    # Configuração das 7 métricas oficiais + 2 opcionais
    metrics_spec = [
        (
            "Propostas",
            "COUNT(DISTINCT id_proposta)",
            "Total de propostas únicas submetidas no Siconv/Transferegov."
        ),
        (
            "Convênios formalizados",
            "COUNT(DISTINCT numero_convenio)",
            "Total de convênios formalizados com número de instrumento emitido."
        ),
        (
            "Propostas com convênio (%)",
            """CASE
    WHEN COUNT(DISTINCT id_proposta) = 0 THEN NULL
    ELSE
        100.0 *
        COUNT(DISTINCT CASE
            WHEN numero_convenio IS NOT NULL THEN id_proposta
        END)
        /
        COUNT(DISTINCT id_proposta)
END""",
            "Proporção das propostas presentes no conjunto filtrado que possuem instrumento formalizado."
        ),
        (
            "Valor global proposto",
            "SUM(valor_global_proposta)",
            "Soma do valor global total de todas as propostas no conjunto filtrado."
        ),
        (
            "Valor global conveniado",
            "SUM(valor_global_convenio)",
            "Soma do valor global pactuado nos convênios formalizados."
        ),
        (
            "Valor empenhado",
            "SUM(valor_empenhado_convenio)",
            "Soma do valor empenhado pelo Governo Federal nos convênios formalizados."
        ),
        (
            "Valor desembolsado",
            "SUM(valor_desembolsado_convenio)",
            "Soma do valor financeiramente desembolsado / repassado aos convenentes."
        ),
        (
            "Percentual empenhado",
            """CASE
    WHEN SUM(valor_repasse_convenio) = 0 THEN NULL
    ELSE
        100.0 * SUM(valor_empenhado_convenio)
        / SUM(valor_repasse_convenio)
END""",
            "Percentual empenhado em relação ao valor total de repasse da União."
        ),
        (
            "Percentual desembolsado",
            """CASE
    WHEN SUM(valor_repasse_convenio) = 0 THEN NULL
    ELSE
        100.0 * SUM(valor_desembolsado_convenio)
        / SUM(valor_repasse_convenio)
END""",
            "Percentual efetivamente desembolsado em relação ao repasse pactuado."
        )
    ]

    existing_metrics = {m.metric_name: m for m in tbl.metrics}
    for m_name, m_expr, m_desc in metrics_spec:
        if m_name in existing_metrics:
            m = existing_metrics[m_name]
            m.expression = m_expr
            m.description = m_desc
        else:
            m = SqlMetric(
                metric_name=m_name,
                expression=m_expr,
                description=m_desc,
                metric_type="count" if "COUNT" in m_expr else "sum"
            )
            tbl.metrics.append(m)

    db.session.commit()
    print(f"[3/6] {len(tbl.metrics)} métricas configuradas com sucesso.")

    # 4. Criação dos 12 Slices (7 KPIs + 4 Gráficos + 1 Tabela Detalhada)
    datasource_str = f"{tbl.id}__table"
    dataset_uuid_str = str(tbl.uuid)

    charts_def: List[Dict[str, Any]] = [
        # KPIs
        {
            "slice_name": "KPI — Propostas",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Propostas",
                "subheader": "Total de propostas",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Convênios formalizados",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Convênios formalizados",
                "subheader": "Instrumentos formalizados",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Propostas com convênio (%)",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Propostas com convênio (%)",
                "subheader": "Taxa de formalização",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": ",.2f",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Valor global proposto",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Valor global proposto",
                "subheader": "Valor Proposto (R$)",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Valor global conveniado",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Valor global conveniado",
                "subheader": "Valor Conveniado (R$)",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Valor empenhado",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Valor empenhado",
                "subheader": "Valor Empenhado (R$)",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "KPI — Valor desembolsado",
            "viz_type": "big_number_total",
            "params": {
                "datasource": datasource_str,
                "viz_type": "big_number_total",
                "metric": "Valor desembolsado",
                "subheader": "Valor Desembolsado (R$)",
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
                "y_axis_format": "SMART_NUMBER",
                "time_range": "No filter",
                "extra_form_data": {}
            }
        },
        # Gráficos
        {
            "slice_name": "Propostas por ano",
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "datasource": datasource_str,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "ano_proposta",
                "metrics": ["Propostas"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "filterOptionName": "filter_ano_prop_not_null",
                        "operator": "IS NOT NULL",
                        "subject": "ano_proposta"
                    }
                ],
                "x_axis_sort_asc": True,
                "order_desc": False,
                "x_axis_title": "Ano da Proposta",
                "y_axis_title": "Quantidade de Propostas",
                "y_axis_format": "SMART_NUMBER",
                "row_limit": 100,
                "show_legend": False,
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "Convênios formalizados por ano",
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "datasource": datasource_str,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "ano_assinatura",
                "metrics": ["Convênios formalizados"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "filterOptionName": "filter_ano_ass_not_null",
                        "operator": "IS NOT NULL",
                        "subject": "ano_assinatura"
                    }
                ],
                "x_axis_sort_asc": True,
                "order_desc": False,
                "x_axis_title": "Ano de Assinatura",
                "y_axis_title": "Convênios Formalizados",
                "y_axis_format": "SMART_NUMBER",
                "row_limit": 100,
                "show_legend": False,
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "Propostas por Unidade da Federação",
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "datasource": datasource_str,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "uf",
                "metrics": ["Propostas"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "filterOptionName": "filter_uf_not_null",
                        "operator": "IS NOT NULL",
                        "subject": "uf"
                    }
                ],
                "order_desc": True,
                "x_axis_title": "Unidade Federativa (UF)",
                "y_axis_title": "Quantidade de Propostas",
                "y_axis_format": "SMART_NUMBER",
                "row_limit": 30,
                "show_legend": False,
                "extra_form_data": {}
            }
        },
        {
            "slice_name": "Convênios por órgão concedente",
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "datasource": datasource_str,
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "orgao_concedente",
                "metrics": ["Convênios formalizados"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SIMPLE",
                        "filterOptionName": "filter_orgao_not_null",
                        "operator": "IS NOT NULL",
                        "subject": "orgao_concedente"
                    }
                ],
                "order_desc": True,
                "row_limit": 10,
                "x_axis_title": "Órgão Concedente",
                "y_axis_title": "Convênios Formalizados",
                "y_axis_format": "SMART_NUMBER",
                "show_legend": False,
                "extra_form_data": {}
            }
        },
        # Tabela Detalhada
        {
            "slice_name": "Detalhamento de propostas e convênios",
            "viz_type": "table",
            "params": {
                "datasource": datasource_str,
                "viz_type": "table",
                "query_mode": "raw",
                "all_columns": [
                    "numero_proposta",
                    "nome_proponente",
                    "uf",
                    "municipio",
                    "orgao_concedente",
                    "modalidade",
                    "situacao_proposta",
                    "numero_convenio",
                    "situacao_convenio",
                    "valor_global_proposta",
                    "valor_global_convenio",
                    "valor_empenhado_convenio",
                    "valor_desembolsado_convenio"
                ],
                "adhoc_filters": [],
                "row_limit": 1000,
                "page_length": 25,
                "server_page_length": 25,
                "order_desc": True,
                "show_cell_bars": False,
                "allow_render_html": True,
                "extra_form_data": {}
            }
        }
    ]

    slice_objects: Dict[str, Slice] = {}
    for c_def in charts_def:
        s_name = c_def["slice_name"]
        slc = db.session.query(Slice).filter_by(slice_name=s_name).first()
        if not slc:
            slc = Slice(
                slice_name=s_name,
                viz_type=c_def["viz_type"],
                datasource_type="table",
                datasource_id=tbl.id,
                params=json.dumps(c_def["params"], ensure_ascii=False)
            )
            db.session.add(slc)
            db.session.commit()
            print(f"[4/6] Slice criado: '{s_name}' (ID: {slc.id})")
        else:
            slc.viz_type = c_def["viz_type"]
            slc.datasource_id = tbl.id
            slc.params = json.dumps(c_def["params"], ensure_ascii=False)
            db.session.commit()
            print(f"[4/6] Slice atualizado: '{s_name}' (ID: {slc.id})")
        slice_objects[s_name] = slc

    # 5. Criação do Dashboard e Montagem do Grid
    dash_title = "Transferências Discricionárias e Legais — Visão Geral"
    dash_slug = "transferencias-visao-geral"

    dashboard = db.session.query(Dashboard).filter_by(dashboard_title=dash_title).first()
    if not dashboard:
        dashboard = Dashboard(
            dashboard_title=dash_title,
            slug=dash_slug,
            published=True
        )
        db.session.add(dashboard)
        db.session.commit()
        print(f"[5/6] Dashboard criado: '{dash_title}' (ID: {dashboard.id})")
    else:
        dashboard.slug = dash_slug
        dashboard.published = True
        db.session.commit()
        print(f"[5/6] Dashboard existente validado: '{dash_title}' (ID: {dashboard.id})")

    # Associação dos slices ao dashboard
    dashboard.slices = list(slice_objects.values())

    # Construção do layout em grid 12 colunas
    all_slice_ids = [s.id for s in slice_objects.values()]

    def make_chart_node(slice_obj: Slice, width: int, height: int, parent_row_id: str) -> Dict[str, Any]:
        c_id = f"CHART-{slice_obj.id}"
        return c_id, {
            "children": [],
            "id": c_id,
            "meta": {
                "chartId": slice_obj.id,
                "height": height,
                "sliceName": slice_obj.slice_name,
                "uuid": str(slice_obj.uuid),
                "width": width
            },
            "parents": ["ROOT_ID", "GRID_ID", parent_row_id],
            "type": "CHART"
        }

    position: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "children": ["GRID_ID"],
            "id": "ROOT_ID",
            "type": "ROOT"
        },
        "HEADER_ID": {
            "id": "HEADER_ID",
            "meta": {"text": dash_title},
            "type": "HEADER"
        },
        "GRID_ID": {
            "children": [
                "ROW-kpi-volumes",
                "ROW-kpi-valores",
                "ROW-charts-ano",
                "ROW-charts-ranking",
                "ROW-tabela-detalhada"
            ],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
            "type": "GRID"
        },
        "ROW-kpi-volumes": {
            "children": [],
            "id": "ROW-kpi-volumes",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW"
        },
        "ROW-kpi-valores": {
            "children": [],
            "id": "ROW-kpi-valores",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW"
        },
        "ROW-charts-ano": {
            "children": [],
            "id": "ROW-charts-ano",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW"
        },
        "ROW-charts-ranking": {
            "children": [],
            "id": "ROW-charts-ranking",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW"
        },
        "ROW-tabela-detalhada": {
            "children": [],
            "id": "ROW-tabela-detalhada",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW"
        }
    }

    # Distribuição dos charts nas linhas
    # Linha 1: 3 KPIs de Volume (largura 4 cada -> 12 colunas)
    vol_kpis = [
        ("KPI — Propostas", 4, 25),
        ("KPI — Convênios formalizados", 4, 25),
        ("KPI — Propostas com convênio (%)", 4, 25)
    ]
    for s_name, w, h in vol_kpis:
        c_id, node = make_chart_node(slice_objects[s_name], w, h, "ROW-kpi-volumes")
        position[c_id] = node
        position["ROW-kpi-volumes"]["children"].append(c_id)

    # Linha 2: 4 KPIs de Valores (largura 3 cada -> 12 colunas)
    val_kpis = [
        ("KPI — Valor global proposto", 3, 25),
        ("KPI — Valor global conveniado", 3, 25),
        ("KPI — Valor empenhado", 3, 25),
        ("KPI — Valor desembolsado", 3, 25)
    ]
    for s_name, w, h in val_kpis:
        c_id, node = make_chart_node(slice_objects[s_name], w, h, "ROW-kpi-valores")
        position[c_id] = node
        position["ROW-kpi-valores"]["children"].append(c_id)

    # Linha 3: Gráficos temporais (largura 6 cada -> 12 colunas)
    ano_charts = [
        ("Propostas por ano", 6, 50),
        ("Convênios formalizados por ano", 6, 50)
    ]
    for s_name, w, h in ano_charts:
        c_id, node = make_chart_node(slice_objects[s_name], w, h, "ROW-charts-ano")
        position[c_id] = node
        position["ROW-charts-ano"]["children"].append(c_id)

    # Linha 4: Gráficos de UF e Órgão (largura 6 cada -> 12 colunas)
    rank_charts = [
        ("Propostas por Unidade da Federação", 6, 50),
        ("Convênios por órgão concedente", 6, 50)
    ]
    for s_name, w, h in rank_charts:
        c_id, node = make_chart_node(slice_objects[s_name], w, h, "ROW-charts-ranking")
        position[c_id] = node
        position["ROW-charts-ranking"]["children"].append(c_id)

    # Linha 5: Tabela Detalhada (largura 12 -> 12 colunas)
    c_id, node = make_chart_node(slice_objects["Detalhamento de propostas e convênios"], 12, 60, "ROW-tabela-detalhada")
    position[c_id] = node
    position["ROW-tabela-detalhada"]["children"].append(c_id)

    dashboard.position_json = json.dumps(position, ensure_ascii=False)

    # Configuração dos 8 Filtros Nativos
    filters_def = [
        ("Ano da proposta", "ano_proposta"),
        ("Ano da assinatura", "ano_assinatura"),
        ("UF", "uf"),
        ("Município", "municipio"),
        ("Órgão concedente", "orgao_concedente"),
        ("Modalidade", "modalidade"),
        ("Situação da proposta", "situacao_proposta"),
        ("Situação do convênio", "situacao_convenio")
    ]

    native_filter_configs = []
    for f_label, f_col in filters_def:
        f_id = f"NATIVE_FILTER-{f_col}"
        native_filter_configs.append({
            "id": f_id,
            "name": f_label,
            "filterType": "filter_select",
            "targets": [
                {
                    "datasetUuid": dataset_uuid_str,
                    "datasetId": tbl.id,
                    "column": {"name": f_col}
                }
            ],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {},
                "ownState": {}
            },
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False
            },
            "cascadeParentIds": [],
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": []
            },
            "type": "NATIVE_FILTER",
            "description": f"Filtrar registros por {f_label}",
            "chartsInScope": all_slice_ids,
            "tabsInScope": []
        })

    json_metadata = {
        "color_scheme": "supersetColors",
        "cross_filters_enabled": True,
        "native_filter_configuration": native_filter_configs,
        "chart_configuration": {},
        "global_chart_configuration": {
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": []
            },
            "chartsInScope": all_slice_ids
        },
        "color_scheme_domain": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": []
    }
    dashboard.json_metadata = json.dumps(json_metadata, ensure_ascii=False)
    db.session.commit()
    print(f"[5/6] Dashboard configurado com layout 12-col e {len(native_filter_configs)} native filters.")

    # 6. Exportação dos Assets para versionamento (YAML e ZIP)
    output_dir = "/app/superset/assets/r5_mvp"
    os.makedirs(output_dir, exist_ok=True)

    print(f"[6/6] Executando exportação oficial do Superset para {output_dir}...")
    cmd = ExportDashboardsCommand([dashboard.id])
    exported_count = 0
    for rel_path, file_content_func in cmd.run():
        full_dest = os.path.join(output_dir, rel_path)
        os.makedirs(os.path.dirname(full_dest), exist_ok=True)
        content = file_content_func()
        with open(full_dest, "w", encoding="utf-8") as f:
            f.write(content)
        exported_count += 1
        print(f"  Exportado: {rel_path}")

    print(f"[6/6] Sucesso! Total de {exported_count} arquivos versionados exportados.")
