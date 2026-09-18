"""
DAG Airflow: r6_transformacoes_lakehouse
Orquestra o pipeline de transformação pós-Bronze do Lakehouse Transferegov.

Camadas e etapas orquestradas:
- Preflight: validação de ambiente e conectividade dbt com Spark Thrift Server.
- Silver: bootstrap de schema, compilação/testes dbt da camada Silver e gate de reconciliação Bronze->Silver.
- Gold: bootstrap de schema (reutilizando bootstrap_silver.py com parâmetros específicos de schema/location),
        compilação/testes dbt dos modelos core (dimensões, fatos e bridges) e gate de reconciliação Silver->Gold (12 gates).
- Serving: compilação da visão semântica (vw_superset_proposta_convenio) e do mart analítico Delta (mart_superset_proposta_convenio).
- Documentation: geração dos metadados e documentação dbt (manifest.json, catalog.json, index.html).

Características operacionais:
- Execução sequencial e fail-fast estrito (trigger_rule padrão all_success).
- Idempotência de ponta a ponta (rerunnable sem efeitos colaterais ou duplicidades).
- Pré-condição operacional: camada Bronze pré-ingerida (R2).
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Constantes de diretórios no container Airflow
DBT_DIR = "/home/airflow/dbt_lakehouse"
SCRIPTS_DIR = "/scripts"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="r6_transformacoes_lakehouse",
    default_args=default_args,
    description="Pipeline R6-A — Orquestração de Transformações Pós-Bronze (Silver -> Gold -> Serving -> Docs)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["r6", "transformacoes", "lakehouse", "dbt", "transferegov"],
) as dag:

    start = EmptyOperator(task_id="start")

    preflight_dbt = BashOperator(
        task_id="preflight_dbt",
        bash_command=f"cd {DBT_DIR} && dbt debug --profiles-dir .",
    )

    with TaskGroup(group_id="silver") as silver_group:
        bootstrap_silver = BashOperator(
            task_id="bootstrap_silver",
            bash_command=(
                f"python {SCRIPTS_DIR}/bootstrap_silver.py "
                f"--schema silver "
                f"--location s3a://silver/warehouse"
            ),
        )

        dbt_build_silver = BashOperator(
            task_id="dbt_build_silver",
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt build --select path:models/silver --exclude '*gold*' '*mart*' '*superset*' '*r5*' --profiles-dir ."
            ),
        )

        reconcile_bronze_silver = BashOperator(
            task_id="reconcile_bronze_silver",
            bash_command=f"python {SCRIPTS_DIR}/reconcile_bronze_silver.py",
        )

        bootstrap_silver >> dbt_build_silver >> reconcile_bronze_silver

    with TaskGroup(group_id="gold") as gold_group:
        # Reutilização do script genérico scripts/bootstrap_silver.py parametrizado para o schema gold
        bootstrap_gold = BashOperator(
            task_id="bootstrap_gold",
            bash_command=(
                f"python {SCRIPTS_DIR}/bootstrap_silver.py "
                f"--schema gold "
                f"--location s3a://gold/warehouse"
            ),
        )

        dbt_build_gold_core = BashOperator(
            task_id="dbt_build_gold_core",
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt build --select path:models/gold/dimensions path:models/gold/facts path:models/gold/bridges "
                f"--exclude '*mart*' '*superset*' '*r5*' "
                f"--profiles-dir ."
            ),
        )

        reconcile_silver_gold = BashOperator(
            task_id="reconcile_silver_gold",
            bash_command=f"python {SCRIPTS_DIR}/reconcile_silver_gold.py",
        )

        bootstrap_gold >> dbt_build_gold_core >> reconcile_silver_gold

    with TaskGroup(group_id="serving") as serving_group:
        dbt_build_semantic_view = BashOperator(
            task_id="dbt_build_semantic_view",
            bash_command=f"cd {DBT_DIR} && dbt build --select vw_superset_proposta_convenio --exclude '*mart*' --profiles-dir .",
        )

        dbt_build_serving_mart = BashOperator(
            task_id="dbt_build_serving_mart",
            bash_command=f"cd {DBT_DIR} && dbt build --select mart_superset_proposta_convenio --profiles-dir .",
        )

        dbt_build_semantic_view >> dbt_build_serving_mart

    with TaskGroup(group_id="documentation") as documentation_group:
        dbt_docs_generate = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=f"cd {DBT_DIR} && dbt docs generate --profiles-dir .",
        )

    end = EmptyOperator(task_id="end")

    start >> preflight_dbt >> silver_group >> gold_group >> serving_group >> documentation_group >> end
