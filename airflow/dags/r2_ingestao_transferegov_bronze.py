"""
DAG Airflow: r2_ingestao_transferegov_bronze
Orquestra a ingestão da camada Bronze das transferências do Transferegov.
"""
import os
from datetime import datetime
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

# Template seguro para ID de execução sem caracteres inválidos em sistemas de arquivos
RUN_ID_TEMPLATE = "run_{{ ts_nodash }}"
FORCE_FLAG_TEMPLATE = "{% if params.force %}--force{% endif %}"

def decide_branch(**context):
    """
    Avalia a decisão da etapa de controle inicial persistida no arquivo de estado da execução.
    Se a execução for classificada como NO_CHANGE, segue para finalizar_no_change;
    caso contrário, segue para o pipeline de ingestão sequencial dos datasets analíticos.
    """
    run_id = f"run_{context['ts_nodash']}"
    decision_file = f"/data/staging/{run_id}/decision.txt"
    legacy_file = f"/data/staging/{run_id}_status.txt"
    target_file = decision_file if os.path.exists(decision_file) else legacy_file

    if os.path.exists(target_file):
        with open(target_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content == "STATUS_NO_CHANGE":
            return "finalizar_no_change"
    return "ingestao_siconv_programa"

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
