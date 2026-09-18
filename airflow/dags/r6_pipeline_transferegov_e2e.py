"""
DAG Airflow: r6_pipeline_transferegov_e2e
Orquestrador E2E do Lakehouse de Transferências Discricionárias e Legais (Transferegov).

Coordena:
    1. Ingestão da camada Bronze (r2_ingestao_transferegov_bronze)
    2. Avaliação de decisão via tabela de auditoria persistente (bronze.ingestion_runs)
    3. Ramificação condicional:
        - NO_CHANGE: encerra sem reprocessar as camadas analíticas (preservando estado atual)
        - SUCCESS: dispara a esteira analítica (r6_transformacoes_lakehouse: Silver -> Gold -> Serving -> Docs)
"""
import os
import re
from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from pyhive import hive

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def avaliar_decisao_bronze(**context) -> str:
    """
    Consulta o Spark Thrift Server no catálogo da camada Bronze para inspecionar
    o registro persistente em bronze.ingestion_runs correlacionado ao ingestion_run_id do E2E.

    Semântica estrita:
    - Exactly 1 registro esperado.
    - status == 'NO_CHANGE': retorna 'no_change' (curto-circuito idempotente).
    - status == 'SUCCESS': retorna 'trigger_transformacoes' (execução analítica downstream).
    - Qualquer outro estado ou contagem anômala: raise AirflowException (fail-fast).
    """
    dag_run_conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    ingestion_run_id = dag_run_conf.get("ingestion_run_id") or f"e2e_{context['ts_nodash']}"

    # Validação de segurança contra injeção e caracteres não permitidos em sistemas de arquivos/SQL
    if not re.match(r"^[A-Za-z0-9_.-]+$", ingestion_run_id):
        raise AirflowException(f"ingestion_run_id contém caracteres inválidos: '{ingestion_run_id}'")

    host = os.getenv("SPARK_THRIFT_HOST", "spark-thrift-server")
    port = int(os.getenv("SPARK_THRIFT_PORT", "10000"))
    database = "bronze"

    try:
        conn = hive.Connection(host=host, port=port, username="airflow", database=database)
        cursor = conn.cursor()
        query = f"SELECT status FROM bronze.ingestion_runs WHERE ingestion_run_id = '{ingestion_run_id}'"
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        raise AirflowException(
            f"Erro ao consultar catálogo bronze.ingestion_runs no Thrift Server ({host}:{port}): {e}"
        ) from e

    if not rows:
        raise AirflowException(
            f"Nenhum registro de auditoria encontrado em bronze.ingestion_runs para ingestion_run_id='{ingestion_run_id}'"
        )
    if len(rows) > 1:
        raise AirflowException(
            f"Múltiplos registros ({len(rows)}) encontrados em bronze.ingestion_runs para ingestion_run_id='{ingestion_run_id}'"
        )

    status = rows[0][0]
    if status == "NO_CHANGE":
        return "no_change"
    elif status == "SUCCESS":
        return "trigger_transformacoes"
    else:
        raise AirflowException(
            f"Status inesperado ou inválido em bronze.ingestion_runs: '{status}' para ingestion_run_id='{ingestion_run_id}'"
        )


with DAG(
    dag_id="r6_pipeline_transferegov_e2e",
    default_args=default_args,
    description="Pipeline R6-B — Orquestrador E2E Transferegov -> Bronze -> Silver -> Gold -> Serving",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "force_bronze": Param(
            default=False,
            type="boolean",
            description="Forçar nova verificação e re-download no Bronze mesmo com data_carga idêntica"
        )
    },
    tags=["r6", "e2e", "orquestracao", "lakehouse", "transferegov"]
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id="r2_ingestao_transferegov_bronze",
        trigger_run_id="r6b_bronze__{{ ts_nodash }}",
        conf={
            "force": "{{ params.force_bronze }}",
            "ingestion_run_id": "e2e_{{ ts_nodash }}",
            "e2e_parent_run_id": "{{ run_id }}",
        },
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    decidir_pos_bronze = BranchPythonOperator(
        task_id="decidir_pos_bronze",
        python_callable=avaliar_decisao_bronze,
    )

    no_change = EmptyOperator(
        task_id="no_change",
    )

    trigger_transformacoes = TriggerDagRunOperator(
        task_id="trigger_transformacoes",
        trigger_dag_id="r6_transformacoes_lakehouse",
        trigger_run_id="r6b_transform__{{ ts_nodash }}",
        conf={
            "e2e_parent_run_id": "{{ run_id }}",
            "source_ingestion_run_id": "e2e_{{ ts_nodash }}",
        },
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=True,
    )

    # NONE_FAILED_MIN_ONE_SUCCESS é estritamente necessária para a convergência após branching:
    # O BranchPythonOperator ('decidir_pos_bronze') executa apenas um ramo, marcando o outro como SKIPPED.
    # Se all_success fosse usado, 'end' seria indevidamente marcado como SKIPPED.
    # Com NONE_FAILED_MIN_ONE_SUCCESS:
    #   - Se ramo no_change for SUCCESS e trigger_transformacoes for SKIPPED -> end é SUCCESS.
    #   - Se ramo trigger_transformacoes for SUCCESS e no_change for SKIPPED -> end é SUCCESS.
    #   - Se qualquer ramo falhar (ex: trigger_transformacoes falhar) -> end NÃO executa e a DAG falha.
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Topologia da DAG E2E
    start >> trigger_bronze >> decidir_pos_bronze
    decidir_pos_bronze >> no_change >> end
    decidir_pos_bronze >> trigger_transformacoes >> end
