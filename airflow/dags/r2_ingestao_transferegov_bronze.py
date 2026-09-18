"""
DAG Airflow: r2_ingestao_transferegov_bronze
Orquestra a ingestão da camada Bronze das transferências do Transferegov.
"""
import os
import re
from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

# Template seguro para ID de execução: usa ingestion_run_id se informado, senão fallback run_{{ ts_nodash }}
RUN_ID_TEMPLATE = (
    "{% if params.ingestion_run_id and params.ingestion_run_id|string|trim != '' %}"
    "{{ params.ingestion_run_id|string|trim }}"
    "{% else %}"
    "run_{{ ts_nodash }}"
    "{% endif %}"
)
FORCE_FLAG_TEMPLATE = "{% if params.force %}--force{% endif %}"

def get_ingestion_run_id(context: dict) -> str:
    """
    Resolve o identificador de execução de forma determinística e consistente.
    Se params.ingestion_run_id for fornecido e não vazio, utiliza-o (validando caracteres seguros).
    Caso contrário, mantém o padrão retrocompatível run_{ts_nodash}.
    """
    params = context.get("params", {})
    val = params.get("ingestion_run_id")
    if val and str(val).strip():
        clean_val = str(val).strip()
        if not re.match(r"^[A-Za-z0-9_.-]+$", clean_val):
            raise ValueError(f"ingestion_run_id contém caracteres inválidos para sistema de arquivos: '{clean_val}'")
        return clean_val
    return f"run_{context['ts_nodash']}"

def decide_branch(**context):
    """
    Avalia a decisão da etapa de controle inicial persistida no arquivo de estado da execução.
    Se a execução for classificada como NO_CHANGE, segue para finalizar_no_change;
    caso contrário, segue para o pipeline de ingestão sequencial dos datasets analíticos.
    """
    run_id = get_ingestion_run_id(context)
    decision_file = f"/data/staging/{run_id}/decision.txt"
    legacy_file = f"/data/staging/{run_id}_status.txt"
    target_file = decision_file if os.path.exists(decision_file) else legacy_file

    if os.path.exists(target_file):
        with open(target_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content == "STATUS_NO_CHANGE":
            return "finalizar_no_change"
    return "ingestao_siconv_programa"

def verificar_resultado_r2(**context):
    """
    Finding R6-B-P0: Preserva o resultado real de falha das tarefas críticas após limpeza_temporarios (ALL_DONE).
    Inspeciona todas as TaskInstances do DagRun atual. Se qualquer tarefa anterior
    tiver estado FAILED ou UPSTREAM_FAILED (incluindo limpeza_temporarios), lança AirflowException
    para garantir que a DAG R2 termine com status FAILED e que o TriggerDagRunOperator downstream
    detecte a falha e bloqueie as transformações.
    Tarefas com estado SKIPPED são esperadas e aceitas devido à ramificação condicional.
    """
    dag_run = context.get("dag_run")
    ti_current = context.get("ti")
    current_task_id = ti_current.task_id if ti_current else "finalizar_execucao_r2"

    if not dag_run:
        return

    failed_tasks = []
    for ti in dag_run.get_task_instances():
        if ti.task_id == current_task_id:
            continue
        if ti.state in (TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED, "failed", "upstream_failed"):
            failed_tasks.append(f"{ti.task_id} ({ti.state})")

    if failed_tasks:
        falhas_str = ", ".join(failed_tasks)
        raise AirflowException(
            f"Falha operacional detectada na DAG R2. As seguintes tarefas falharam: {falhas_str}"
        )

with DAG(
    dag_id="r2_ingestao_transferegov_bronze",
    default_args=default_args,
    description="Pipeline R2 — Ingestão/Bronze das fontes oficiais Transferegov",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "force": Param(
            default=False,
            type="boolean",
            description="Forçar nova verificação e re-download mesmo que data_carga seja idêntica"
        ),
        "ingestion_run_id": Param(
            default="",
            type="string",
            pattern=r"^[A-Za-z0-9_.-]*$",
            description="Identificador único da execução para rastreabilidade E2E (opcional, caracteres seguros: A-Za-z0-9_.-)"
        )
    },
    tags=["bronze", "r2", "transferegov", "lakehouse"]
) as dag:

    preflight_check = BashOperator(
        task_id="preflight_check",
        bash_command=f"python /app/ingest_transferegov.py --action preflight --run-id {RUN_ID_TEMPLATE}"
    )

    controle_inicial = BashOperator(
        task_id="controle_inicial",
        bash_command=f"python /app/ingest_transferegov.py --action control_initial --run-id {RUN_ID_TEMPLATE} {FORCE_FLAG_TEMPLATE}",
        do_xcom_push=True
    )

    avaliar_necessidade = BranchPythonOperator(
        task_id="avaliar_necessidade",
        python_callable=decide_branch
    )

    finalizar_no_change = BashOperator(
        task_id="finalizar_no_change",
        bash_command="echo 'Execução concluída com status NO_CHANGE: dados oficiais e estado local inalterados.'"
    )

    ingestao_siconv_programa = BashOperator(
        task_id="ingestao_siconv_programa",
        bash_command=(
            f"python /app/ingest_transferegov.py --action ingest_dataset "
            f"--dataset siconv_programa --run-id {RUN_ID_TEMPLATE} {FORCE_FLAG_TEMPLATE}"
        )
    )

    ingestao_siconv_programa_proposta = BashOperator(
        task_id="ingestao_siconv_programa_proposta",
        bash_command=(
            f"python /app/ingest_transferegov.py --action ingest_dataset "
            f"--dataset siconv_programa_proposta --run-id {RUN_ID_TEMPLATE} {FORCE_FLAG_TEMPLATE}"
        )
    )

    ingestao_siconv_proposta = BashOperator(
        task_id="ingestao_siconv_proposta",
        bash_command=(
            f"python /app/ingest_transferegov.py --action ingest_dataset "
            f"--dataset siconv_proposta --run-id {RUN_ID_TEMPLATE} {FORCE_FLAG_TEMPLATE}"
        )
    )

    ingestao_siconv_convenio = BashOperator(
        task_id="ingestao_siconv_convenio",
        bash_command=(
            f"python /app/ingest_transferegov.py --action ingest_dataset "
            f"--dataset siconv_convenio --run-id {RUN_ID_TEMPLATE} {FORCE_FLAG_TEMPLATE}"
        )
    )

    controle_final = BashOperator(
        task_id="controle_final",
        bash_command=f"python /app/ingest_transferegov.py --action control_final --run-id {RUN_ID_TEMPLATE}"
    )

    validacao_global = BashOperator(
        task_id="validacao_global",
        bash_command=f"python /app/ingest_transferegov.py --action validate_global --run-id {RUN_ID_TEMPLATE}"
    )

    retencao_raw = BashOperator(
        task_id="retencao_raw",
        bash_command=f"python /app/ingest_transferegov.py --action retention --run-id {RUN_ID_TEMPLATE}"
    )

    limpeza_temporarios = BashOperator(
        task_id="limpeza_temporarios",
        bash_command=f"python /app/ingest_transferegov.py --action cleanup --run-id {RUN_ID_TEMPLATE}",
        trigger_rule=TriggerRule.ALL_DONE
    )

    finalizar_execucao_r2 = PythonOperator(
        task_id="finalizar_execucao_r2",
        python_callable=verificar_resultado_r2,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Definição das dependências do fluxo
    preflight_check >> controle_inicial >> avaliar_necessidade

    # Ramo 1: NO_CHANGE
    avaliar_necessidade >> finalizar_no_change >> limpeza_temporarios

    # Ramo 2: Carga efetiva sequencial dos datasets analíticos
    (
        avaliar_necessidade
        >> ingestao_siconv_programa
        >> ingestao_siconv_programa_proposta
        >> ingestao_siconv_proposta
        >> ingestao_siconv_convenio
        >> controle_final
        >> validacao_global
        >> retencao_raw
        >> limpeza_temporarios
    )

    # Gate final de validação operacional (leaf task)
    limpeza_temporarios >> finalizar_execucao_r2
